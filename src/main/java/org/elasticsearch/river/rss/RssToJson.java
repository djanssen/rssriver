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

import com.sun.syndication.feed.synd.SyndContent;
import java.util.Date;
import java.util.List;
import org.jdom.Element;
import com.sun.syndication.feed.module.georss.GeoRSSModule;
import com.sun.syndication.feed.module.georss.GeoRSSUtils;
import com.sun.syndication.feed.module.georss.geometries.Position;
import com.sun.syndication.feed.synd.SyndEntry;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class RssToJson {
	public static XContentBuilder toJson(SyndEntry message, String riverName, String feedName) throws IOException {
        final Map<String, Object> latitude = getPosition(message);
        final List<SyndContent> contents = message.getContents();


        XContentBuilder out = jsonBuilder().startObject();

        out.field("feedname", feedName);
        out.field("title", message.getTitle());
        out.field("author", message.getAuthor());
	out.field("description", message.getDescription() != null ? message.getDescription().getValue() : null);
	out.field("link", message.getLink());
	out.field("publishedDate", message.getPublishedDate());
         out.field("source", message.getSource());
        if (latitude.size() > 0) {
            out.field("location", latitude);
        }
        if (riverName != null) {
            out.field("river", riverName);
        }

      
        // process foreign markup elements
        // following the index destination mapping
        List<Element> foreignMarkups = (List<Element>) message.getForeignMarkup();

        if (foreignMarkups != null)
        {
            String lastPrefix = null;
            for (Element foreignMarkup : foreignMarkups) {
                String prefix = foreignMarkup.getNamespacePrefix();
                String fieldName = foreignMarkup.getName();
                String fieldValue = foreignMarkup.getValue();

                if (lastPrefix != prefix) {

                    if (lastPrefix != null && !lastPrefix.equals("")) {
                        out = out.endObject();
                    }
                    if (prefix != null && !prefix.equals("")) {
                        out = out.startObject(prefix);
                    }
                }
         
                lastPrefix = prefix;
                out.field(fieldName, fieldValue);
            }
            if (lastPrefix != null && !lastPrefix.equals("")) {
                out = out.endObject();
           }
        }

       
        
        return out.endObject();
    }

    private static Map<String, Object> getPosition(SyndEntry message) {
        GeoRSSModule geoRSSModule = GeoRSSUtils.getGeoRSS(message);
        final Map<String, Object> latitude = new HashMap<String, Object>();
        if (geoRSSModule != null) {
            final Position position = geoRSSModule.getPosition();
            if (position != null) {
                latitude.put("lat", position.getLatitude());
                latitude.put("lon", position.getLongitude());
            }
        }
        return latitude;
    }
}


