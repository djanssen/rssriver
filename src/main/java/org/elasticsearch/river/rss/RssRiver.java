/*
 * Licensed to David Pilato (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.rss;

import java.util.LinkedHashMap;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import java.util.Set;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.action.search.SearchType;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.sun.syndication.feed.synd.SyndLinkImpl;
import java.util.List;
import java.util.HashSet;
import com.sun.syndication.feed.rss.Channel;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.Proxy;
import java.net.InetSocketAddress;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.rss.RssToJson.toJson;

/**
 * @author dadoonet (David Pilato)
 */
public class RssRiver extends AbstractRiverComponent implements River {

	private final Client client;

	private final String indexName;

	private final String typeName;

        private final String proxyhost;

        private final int proxyport;


	private volatile ArrayList<Thread> threads;

	private volatile boolean closed = false;

	private final ArrayList<RssRiverFeedDefinition> feedsDefinition;

	@SuppressWarnings({ "unchecked" })
	@Inject
	public RssRiver(RiverName riverName, RiverSettings settings, Client client)
			throws MalformedURLException {
		super(riverName, settings);
		this.client = client;

		if (settings.settings().containsKey("rss")) {
			Map<String, Object> rssSettings = (Map<String, Object>) settings.settings().get("rss");

                        proxyhost = XContentMapValues.nodeStringValue(rssSettings.get("proxyhost"), null);
                        proxyport = XContentMapValues.nodeIntegerValue(rssSettings.get("proxyport"), 3128);

			// Getting feeds array
			boolean array = XContentMapValues.isArray(rssSettings.get("feeds"));
			if (array) {
				ArrayList<Map<String, Object>> feeds = (ArrayList<Map<String, Object>>) rssSettings.get("feeds");
				feedsDefinition = new ArrayList<RssRiverFeedDefinition>(feeds.size());
                                HashSet feedNames = new HashSet(feeds.size());
				for (Map<String, Object> feed : feeds) {
					String url = XContentMapValues.nodeStringValue(feed.get("url"), null);
					String feedname = XContentMapValues.nodeStringValue(feed.get("name"), url);
					int updateRate  = XContentMapValues.nodeIntegerValue(feed.get("update_rate"), 15 * 60 * 1000);
                                        boolean ignoreTtl = XContentMapValues.nodeBooleanValue(feed.get("ignore_ttl"), false);
                                        boolean incrementalDates = XContentMapValues.nodeBooleanValue(feed.get("incremental_dates"), false);
                			String startDate = XContentMapValues.nodeStringValue(feed.get("start_date"), null);
		                    
                                        if (feedNames.contains(feedname)) {
                                            feedname = UUID.nameUUIDFromBytes(feedname.getBytes()).toString();
                                        }
                                        feedNames.add(feedname);
					
                                        feedsDefinition.add(new RssRiverFeedDefinition(feedname, url, updateRate, ignoreTtl, incrementalDates, startDate));
				}
				
			} else {
				logger.warn("rss.url and rss.update_rate have been deprecated. Use rss.feeds[].url and rss.feeds[].update_rate instead.");
				logger.warn("See https://github.com/dadoonet/rssriver/issues/6 for more details...");
				String url = XContentMapValues.nodeStringValue(rssSettings.get("url"), null);
				int updateRate  = XContentMapValues.nodeIntegerValue(rssSettings.get("update_rate"), 15 * 60 * 1000);
                                boolean ignoreTtl = XContentMapValues.nodeBooleanValue("ignore_ttl", false);
                                String feedname = XContentMapValues.nodeStringValue(rssSettings.get("name"), url);
				feedsDefinition = new ArrayList<RssRiverFeedDefinition>(1);
				feedsDefinition.add(new RssRiverFeedDefinition(feedname, url, updateRate, ignoreTtl, false, null));
			}
			
		} else {
			String url = "http://www.lemonde.fr/rss/une.xml";
			logger.warn("You didn't define the rss url. Switching to defaults : [{}]", url);
			int updateRate = 15 * 60 * 1000;
			feedsDefinition = new ArrayList<RssRiverFeedDefinition>(1);
			feedsDefinition.add(new RssRiverFeedDefinition("lemonde", url, updateRate, false, false, null));
                        proxyhost = null;
                        proxyport = 3128;
		}

		
		if (settings.settings().containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings
					.settings().get("index");
			indexName = XContentMapValues.nodeStringValue(
					indexSettings.get("index"), riverName.name());
			typeName = XContentMapValues.nodeStringValue(
					indexSettings.get("type"), "page");
		} else {
			indexName = riverName.name();
			typeName = "page";
		}
	}

	@Override
	public void start() {
		if (logger.isInfoEnabled()) logger.info("Starting rss stream");
		try {
                     ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().filterRoutingTable(true).filterNodes(true);

                     // retrieve indices and aliases
                     Set<String> setIndices= client.admin().cluster().state(clusterStateRequest).actionGet().getState().getMetaData().indices().keySet();
                     Set<String> setAliases= client.admin().cluster().state(clusterStateRequest).actionGet().getState().getMetaData().aliases().keySet();

                     if (!setIndices.contains(indexName) && !setAliases.contains(indexName)) {
                  	client.admin().indices().prepareCreate(indexName).execute()
					.actionGet();
                    }
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// that's fine
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ok, not recovered yet..., lets start indexing and hope we
				// recover by the first bulk
				// TODO: a smarter logic can be to register for cluster event
				// listener here, and only start sampling when the block is
				// removed...
			} else {
				logger.warn("failed to create index [{}], disabling river...",
						e, indexName);
				return;
			}
		}
		
		// We create as many Threads as there are feeds
		threads = new ArrayList<Thread>(feedsDefinition.size());
		int threadNumber = 0;
		for (RssRiverFeedDefinition feedDefinition : feedsDefinition) {
			Thread thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "rss_slurper_" + threadNumber)
                    .newThread(new RSSParser(feedDefinition));
			thread.start();
			threads.add(thread);
			threadNumber++;
		}
	}

	@Override
	public void close() {
		if (logger.isInfoEnabled()) logger.info("Closing rss river");
		closed = true;
		
		// We have to close each Thread
		if (threads != null) {
			for (Thread thread : threads) {
				if (thread != null) {
					thread.interrupt();
				}
			}
		}
	}

	
	private SyndFeed getFeed(String url) {
		try {

                        URLConnection conn = null;
                        if (proxyhost != null) {
                            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyhost, proxyport));
                            conn = new URL(url).openConnection(proxy);
                        } else {
                            conn = new URL(url).openConnection();
                        }

			SyndFeedInput input = new SyndFeedInput();
            input.setPreserveWireFeed(true);
			SyndFeed feed = input.build(new XmlReader(conn));
			return feed;
		} catch (MalformedURLException e) {
			logger.error("RSS Url is incorrect : [{}].", url);
		} catch (IllegalArgumentException e) {
			logger.error("Feed from [{}] is incorrect.", url);
		} catch (FeedException e) {
			logger.error("Can not parse feed from [{}].", url);
		} catch (IOException e) {
			logger.error("Can not read feed from [{}].", url);
		}
		
		return null;
	}
	
	private class RSSParser implements Runnable {
		private String url;
		private int updateRate;
		private String feedname;
                private boolean ignoreTtl;
                private boolean incremental;
                private Date   startDate;
                private final int max_pagging = 10;


                private class RiverUpdatedInfo {
                    public Date lastFeedDate;
                    public Date lastDocDate;
                };


        public RSSParser(String feedname, String url, int updateRate, boolean ignoreTtl, boolean incremental,Date startDate) {
			this.feedname = feedname;
			this.url = url;
			this.updateRate = updateRate;
			this.incremental = incremental;
			this.startDate = startDate;
            this.ignoreTtl = ignoreTtl;
            if (logger.isInfoEnabled()) logger.info("Creating rss stream river [{}] for [{}] every [{}] ms",
                    feedname, url, updateRate);
		}

        public RSSParser(RssRiverFeedDefinition feedDefinition) {
            this(feedDefinition.getFeedname(),
                    feedDefinition.getUrl(),
                    feedDefinition.getUpdateRate(),
                    feedDefinition.isIgnoreTtl(),
                    feedDefinition.isIncrementalDates(),
                    feedDefinition.getStartDate()
                    );
        }

        @SuppressWarnings("unchecked")
		@Override
		public void run() {

            String currentUrl = url;
            int  pagging = 0;

            while (true) {
				if (closed) {
                                        if (logger.isDebugEnabled()) logger.debug("Rss river [{}] closed", feedname);
                			return;
				}
		
                                // get last dates for this feed 
                                String lastupdateField = "_lastupdated_" + feedname;
                                RiverUpdatedInfo riverInfo = RetrieveLastDates(lastupdateField);

                                 if (logger.isDebugEnabled()) logger.debug("lastupdateField  :  {}, {}, {}", lastupdateField, riverInfo.lastFeedDate, riverInfo.lastDocDate);

                                // build incremental Url
                                // we use  ISO8601 UTC format
                                if (incremental && pagging == 0) {
                                    SimpleDateFormat dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                                    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                                    String lastDateStr = dateFormat.format((riverInfo.lastDocDate == null) ?  startDate : riverInfo.lastDocDate);
                                    currentUrl = String.format(url, lastDateStr);
                                }

				// Let's call the Rss flow
                                if (logger.isDebugEnabled()) logger.debug("Resquesting feed url :  {}", currentUrl);
 				SyndFeed feed = getFeed(currentUrl);
                                String nextUrl = null;
                                int  updatedCount = 0;
                if (feed != null) {
                    if (logger.isDebugEnabled()) logger.debug("Reading feed from {}", currentUrl);
                    Date currentFeedDate = feed.getPublishedDate();
                    Date currentDocDate = null;
                    if (logger.isDebugEnabled()) logger.debug("Feed publish date is {}", currentFeedDate);


                    // Get 'next' url (RSS/Atom pagination)
                    for (SyndLinkImpl link : (List<SyndLinkImpl>) feed.getLinks()) {
                        if (link.getRel() != null && link.getRel().equalsIgnoreCase("next")) {
                            nextUrl = link.getHref();
                        }
                    }

               
                    // Comparing dates to see if we have something to do or not
                    if (riverInfo.lastFeedDate == null || incremental || (currentFeedDate != null && currentFeedDate.after(riverInfo.lastFeedDate))) {
                        // We have to send results to ES
                        if (logger.isTraceEnabled()) logger.trace("Feed is updated : {}", feed);

                        BulkRequestBuilder bulk = client.prepareBulk();
                        try {
                            // We have now to send each feed to ES
                            for (SyndEntry message : (Iterable<SyndEntry>) feed.getEntries()) {
                                String description = "";
                                if (message.getDescription() != null) {
                                    description = message.getDescription().getValue();
                                }
                                String entryId = "";
                                if (message.getUri() != null) {
                                    entryId = message.getUri();
                                }
                                if (message.getUpdatedDate() == null) {
                                    message.setUpdatedDate(new Date());
                                }

                                // Let's define the rule for UUID generation
                                String id = (entryId.isEmpty()) ? UUID.nameUUIDFromBytes(description.getBytes()).toString() : entryId;

                                // Let's look if object already exists
                                GetResponse oldMessage = client.prepareGet(indexName, typeName, id).execute().actionGet();
                                if (!oldMessage.isExists()) {
                                    bulk.add(indexRequest(indexName).type(typeName).id(id).source(toJson(message, riverName.getName(), feedname)));
                                    updatedCount++;
                                    //if (logger.isDebugEnabled()) logger.debug("FeedMessage [{}] update detected for source [{}]", id, feedname != null ? feedname : "undefined");
                                    if (logger.isTraceEnabled()) logger.trace("FeedMessage is : {}", message);
                                } else {
                                    if (logger.isTraceEnabled()) logger.trace("FeedMessage {} already exist. Ignoring", id);
                                }
                                // update current date info
                                currentDocDate = message.getUpdatedDate();

                            }

                            if (logger.isTraceEnabled()) {
                                logger.trace("processing [_seq  ]: [{}]/[{}]/[{}], last_seq [{}]", indexName, riverName.name(), lastupdateField, currentFeedDate);
                            }
                        } catch (IOException e) {
                            updatedCount = -1;
                            logger.warn("failed to add feed message entry to bulk indexing");
                        }

                        if (updatedCount > 0)
                        {
                            try {
                                BulkResponse response = bulk.execute().actionGet();
                                if (response.hasFailures()) {
                                    // TODO probably some of them has been updated
                                    updatedCount = -1;
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                }
                            } catch (Exception e) {
                                updatedCount = -1;
                                logger.warn("failed to execute bulk", e);
                            }
                        }

                        if (updatedCount >= 0) {
                            // we store the lastupdate
                            StoreLastDates(lastupdateField, currentFeedDate, currentDocDate);
                        }

                    } else {
                        // Nothing new... Just relax !
                        if (logger.isDebugEnabled()) logger.debug("Nothing new in the feed... Relaxing...");
                    }
		
                    // #8 : Use the ttl rss field to auto adjust feed refresh rate
                    if (!ignoreTtl && feed.originalWireFeed() != null && feed.originalWireFeed() instanceof Channel) {
                        Channel channel = (Channel) feed.originalWireFeed();
                        if (channel.getTtl() > 0) {
                            int ms = channel.getTtl() * 60 * 1000;
                            if (ms != updateRate) {
                                updateRate = ms;
                                if (logger.isInfoEnabled())
                                    logger.info("Auto adjusting update rate with provided ttl: {} mn", channel.getTtl());
                            }
                        }
                    }
                } // end if (feed != null)
                try {
                    if (nextUrl != null && !nextUrl.isEmpty() && updatedCount > 0 && (!incremental || pagging < max_pagging)) {
                        currentUrl = nextUrl;
                        nextUrl = null;
                        pagging++;
                        if (logger.isDebugEnabled()) logger.debug("Rss river is going to ask for next page");
                    } else {
                        pagging = 0;
                        currentUrl = url;
                        nextUrl = null;
                        if (logger.isDebugEnabled()) logger.debug("Rss river [{}] is going to sleep for {} ms", feedname, updateRate);
                        Thread.sleep(updateRate);
                    }
		}
                catch (InterruptedException e1) {
                }
	} // end while(true)
}

        @SuppressWarnings("unchecked")
		private void StoreLastDates(String lastupdateField, Date currentFeedDate,  Date currentDocDate) {
                    // we store the lastupdate
                   BulkRequestBuilder bulk = client.prepareBulk();
                    try {
                        bulk.add(indexRequest("_river").type(riverName.name()).id(lastupdateField)
                                        .source(jsonBuilder().startObject().startObject("rss").field("feed_date", currentFeedDate).field("doc_date", currentDocDate).endObject().endObject()));
                    } catch (IOException e) {
                        logger.warn("failed to add lastupdate  entry to bulk indexing");
                    }
                    try {
                        BulkResponse response = bulk.execute().actionGet();
                        if (response.hasFailures()) {
                            logger.warn("lastupdate : failed to execute" + response.buildFailureMessage());
                        }
                    } catch (Exception e) {
                        logger.warn("lastupdate : failed to execute bulk", e);
                    }
            }

        @SuppressWarnings("unchecked")
		private RiverUpdatedInfo RetrieveLastDates(String lastupdateField) {
                RiverUpdatedInfo result = new RiverUpdatedInfo();
                try {
                // Do something
                client.admin().indices().prepareRefresh("_river").execute().actionGet();
                GetResponse lastSeqGetResponse =
                        client.prepareGet("_river", riverName().name(), lastupdateField).execute().actionGet();
                if (lastSeqGetResponse.isExists()) {
                    Map<String, Object> rssState = (Map<String, Object>) lastSeqGetResponse.getSourceAsMap().get("rss");

                    if (rssState != null) {
                        Object lastfeed_date = rssState.get("feed_date");
                        if (lastfeed_date != null) {
                            String strLastDate = lastfeed_date.toString();
                            result.lastFeedDate = ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(strLastDate).toDate();
                        }
                        Object lastdoc_date = rssState.get("doc_date");
                        if (lastdoc_date != null) {
                            String strLastDate = lastdoc_date.toString();
                            result.lastDocDate = ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(strLastDate).toDate();
                        }
                    }
                } else {
                    // First call
                    if (logger.isDebugEnabled()) logger.debug("{} doesn't exist, trying to find previous {} within {} from the same feed {}", lastupdateField, typeName, indexName, feedname);

                    SearchRequestBuilder searchBuilder = client.prepareSearch(indexName);
                    if (searchBuilder != null) {

                        ClusterState cs = client.admin().cluster().prepareState().setFilterIndices(indexName).execute().actionGet().getState();
                        IndexMetaData imd = cs.getMetaData().index(indexName);
                        MappingMetaData metad = imd.mapping(typeName);

                         if (metad != null) {
                             LinkedHashMap<String, Object> structure = (LinkedHashMap<String, Object>)metad.getSourceAsMap().get("properties");
                            if  (structure != null && structure.containsKey("feedDate")) {
                        

                                SearchHits lastDocs = searchBuilder.setTypes(typeName)
                                                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                                                        .setQuery(QueryBuilders.termQuery("feedname", feedname))
                                                        .addSort("feedDate", SortOrder.DESC)   // Sort by feedDate
                                                        .setSize(1)
                                                        .execute()
                                                        .actionGet().getHits();
                                if (lastDocs != null && lastDocs.getTotalHits() > 0) {
                                    result.lastDocDate  = ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(lastDocs.getAt(0).getSource().get("updatedDate").toString()).toDate();
                                }
                             }
                        }
                    }

                }
            } catch (Exception e) {
                logger.warn("failed to get _lastupdate, throttling....", e);
            }
            return result;
        }
    }
}
