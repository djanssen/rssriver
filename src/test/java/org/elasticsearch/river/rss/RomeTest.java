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

import org.elasticsearch.common.xcontent.XContentBuilder;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.rss.Channel;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.elasticsearch.river.rss.RssToJson.toJson;

public class RomeTest {

	@Test
	public void test() throws Exception {
		String url = "http://www.lemonde.fr/rss/une.xml";
		URL feedUrl = new URL(url);

		SyndFeedInput input = new SyndFeedInput();
        input.setPreserveWireFeed(true);
		SyndFeed feed = input.build(new XmlReader(feedUrl));

		assertNotNull(feed);
		assertFalse(feed.getEntries().isEmpty());





               for (SyndEntry message : (Iterable<SyndEntry>) feed.getEntries()) {

                    XContentBuilder json = toJson(message, "rometest", "rometest");
                    String text = json.string();
                    String keep = text;
              }














        assertNotNull(feed.originalWireFeed());
        assertTrue(feed.originalWireFeed() instanceof Channel);

        Channel channel = (Channel) feed.originalWireFeed();
        assertTrue(channel.getTtl() >= 0);
	}
}
