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

import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.river.rss.RssToJson.toJson;
import static org.junit.Assert.*;

public class RssToJsonTest {

    public static final String JSON = "{\"feedname\":null,\"title\":\"title\",\"author\":\"\",\"description\":\"desc\",\"link\":\"http://link.com/abc\",\"publishedDate\":\"2011-11-10T06:29:02.000Z\",\"updatedDate\":null,\"source\":null,\"location\":{\"lon\":12.4839019775391,\"lat\":41.8947384616695}}";
    /*
     *      RSS 2.0 -> JSON example
     *
     *
     * <channel>
     *   ...
     *   <item>
     *     <title>A simple example 1</title>
     *     <description>A simple example 1</description>
     * 	   <guid>http://some.server.com/weblogItem3207</guid>
     *     <airplanes:banking>Turn the rudder 45 degrees to the left.</airplanes:banking>
     *     <airplanes:color>blue</airplanes:color>
     *     <airplanes:date>2011-11-10T09:21:15Z</airplanes:date>
     *     <financial:banking>Bank of Montreal</financial:banking>
     *   </item>
     *   ...
     *
     *
     *  {
     *      "feedname": null,
     *      "title": "A simple example 1",
     *      "author": "",
     *      "description": "A simple example 1",
     *      "link": null,
     *      "publishedDate": null,
     *      "updatedDate": null,
     *      "source": null,
     *      "airplanes": {
     *          "banking": "Turn the rudder 45 degrees to the left.",
     *          "color": "blue",
     *          "date": "2011-11-10T09:21:15Z"
     *      },
     *      "financial": {
     *          "banking": "Bank of Montreal"
     *          }
     *  }
     */
    public static final String[] JSON_2 = { "{\"feedname\":null,\"title\":\"A simple example 1\",\"author\":\"\",\"description\":\"A simple example 1\",\"link\":null,\"publishedDate\":null,\"updatedDate\":null,\"source\":null,\"airplanes\":{\"banking\":\"Turn the rudder 45 degrees to the left.\",\"color\":\"blue\",\"date\":\"2011-11-10T09:21:15Z\"},\"financial\":{\"banking\":\"Bank of Montreal\"}}", 
                                            "{\"feedname\":null,\"title\":\"A simple example 2\",\"author\":\"\",\"description\":\"A simple example 2\",\"link\":null,\"publishedDate\":null,\"updatedDate\":null,\"source\":null,\"airplanes\":{\"banking\":\"Turn the rudder 90 degrees to the right.\",\"color\":\"blue\"},\"financial\":{\"banking\":\"Bank of London\"}}"};


    @Test /* this test should be moved somewhere else */
	public void shouldParseRss1_0() throws Exception {
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new XmlReader(getClass().getResource("/rss.xml")));
        assertTrue(feed.getEntries().size() > 0);
        for (Object o : feed.getEntries()) {
            SyndEntryImpl message = (SyndEntryImpl) o;
            XContentBuilder xcb = toJson(message, null, null);
            assertNotNull(xcb);
        }
	}

    @Test /* this test should be moved somewhere else */
	public void shouldParseRss2_0() throws Exception {
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new XmlReader(getClass().getResource("/rss2.xml")));
        assertTrue(feed.getEntries().size() > 0);
        int i = 0;
        for (Object o : feed.getEntries()) {
            SyndEntryImpl message = (SyndEntryImpl) o;
            XContentBuilder xcb = toJson(message, null, null);
            String jsonTxt = xcb.string();
            assertEquals(JSON_2[i], jsonTxt);
            assertNotNull(xcb);
            i++;
        }
	}

    @Test
    public void shouldParseRssGeoInformation() throws Exception {
        final SyndEntryImpl entry = buildEntry();
        final XContentBuilder xContentBuilder = RssToJson.toJson(entry, null, null);
        String jsonTxt = xContentBuilder.string();
        assertEquals(JSON, jsonTxt);
    }

    private SyndEntryImpl buildEntry() throws FeedException, IOException {
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new XmlReader(getClass().getResource("/rss.xml")));
        return (SyndEntryImpl) feed.getEntries().get(0);
    }

}
