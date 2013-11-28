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

import org.jdom.Attribute;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Date;
import com.sun.syndication.feed.synd.SyndContent;
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
       enum ElementType {ROOT, ARRAY, NAMED} ;

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
        out.field("updatedDate", message.getUpdatedDate());
        out.field("feedDate", new Date());
        out.field("source", message.getSource());
        if (latitude.size() > 0) {
            out.field("location", latitude);
        }
        if (riverName != null) {
            out.field("river", riverName);
        }

        // process first content only
        if (contents != null && !contents.isEmpty()) {
            out.field("content", contents.get(0).getValue());
        }
      
        // process foreign markup elements
        // following the index destination mapping
        List<Element> foreignMarkups = (List<Element>) message.getForeignMarkup();
        Element2Json(ElementType.ROOT,  null, null, new ArrayList<Attribute>(), foreignMarkups, out);
       
        
        return out.endObject();
    }


    private static  void Element2Json(ElementType elementType, String elementName, String elementValue, List<Attribute> attributes, List<Element> childs, XContentBuilder out) throws IOException
    {

        // simple element wihtout any children or attributes
        if (attributes.isEmpty() && childs.isEmpty())
        {
            String stringValue = trim(elementValue," \t\n");
            Object objectValue =  JSONValue(stringValue);
            if (!stringValue.isEmpty() && objectValue != null) {
                if (elementType != ElementType.ARRAY)
                    out.field(elementName, objectValue);
                else
                    out.value(objectValue);
            }
            return;
        }
 
        if (elementType != ElementType.ROOT)
        {
            if (elementType == ElementType.ARRAY)
                out = out.startObject();
            if (elementType == ElementType.NAMED)
                out = out.startObject(elementName);
        }

        // handle attributes
         for (Attribute attribute : attributes)
         {
            String attributePrefix = attribute.getNamespacePrefix();
            String attributeName = (attributePrefix.isEmpty()) ? attribute.getName() : attributePrefix + ":" + attribute.getName();
            String stringValue = trim(attribute.getValue()," \t\n");
            Object attributeValue =  JSONValue(stringValue);
            if (!stringValue.isEmpty() && attributeValue != null) {
                out.field(attributeName, attributeValue);
            }
         }

         Map childsByName = new Hashtable() ;
        // build elements list for each child name 
        for (Element child : childs) {
            String childPrefix = child.getNamespacePrefix();
            String childName = (childPrefix.isEmpty()) ? child.getName() : childPrefix + ":" + child.getName();
            ArrayList<Element> childValues =  (ArrayList<Element>) childsByName.get(childName);
            if (childValues == null)
                childValues = new ArrayList<Element>();
            childValues.add(child);
            childsByName.put(childName, childValues);
        }

        Iterator it = childsByName.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            String childName = (String) pairs.getKey();
            ArrayList<Element> childValues  = ( ArrayList<Element>) pairs.getValue();

            // single element
            boolean isArray =  childValues.size() > 1;
            if(childValues.size() == 1)
            {
                String childValue = childValues.get(0).getValue();
                List<Attribute> childAttributes = childValues.get(0).getAttributes();
                List<Element> childChilds = childValues.get(0).getChildren();
                Element2Json(ElementType.NAMED, childName, childValue, childAttributes, childChilds, out);
            }
            // array
            else {
                out.startArray(childName);
                for (Element childvalue : childValues) {
                    String childValue = childvalue.getValue();
                    List<Attribute> childAttributes = childvalue.getAttributes();
                    List<Element> childChilds = childvalue.getChildren();
                    Element2Json(ElementType.ARRAY, childName, childValue, childAttributes, childChilds, out);
                }
                out.endArray();
            }
        }

        if (elementType != ElementType.ROOT)
           out = out.endObject();
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

    private static Object JSONValue(String value)
    {
        // null object
        if (value == null) {
             return null;
        }

        // boolean object
        if(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))
            return Boolean.parseBoolean(value);

        // 'number' object
        try {  return Short.parseShort(value); }
        catch(NumberFormatException nfe) {}

        try {  return Integer.parseInt(value); }
        catch(NumberFormatException nfe) {}

        try {  return Long.parseLong(value); }
        catch(NumberFormatException nfe) {}

        try {  return Float.parseFloat(value); }
        catch(NumberFormatException nfe) {}

        try {  return Double.parseDouble(value); } 
        catch(NumberFormatException nfe) {}

        // return string object by default
        return value;
    }

     private static String trim(String s, String toTrim)
     {
        int originalLen = s.length();
        int len = s.length();
        int st = 0;

        while ((st < len) && (toTrim.indexOf(s.charAt(st)) != -1)) {
            st++;
        }
        while ((st < len) && (toTrim.indexOf(s.charAt(len - 1)) != -1)) {
            len--;
        }
        return ((st > 0) || (len < originalLen)) ? s.substring(st, len) : s.toString();
    }
}


