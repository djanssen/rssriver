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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

/**
 * Define an RSS Feed with source (aka short name), url and updateRate attributes
 * @author dadoonet (David Pilato)
 * @since 0.0.5
 */
public class RssRiverFeedDefinition {
	private String feedname;
	private String url;
	private int updateRate;
        private boolean ignoreTtl = false;
	private boolean incrementalDates = false;
        private Date startDate;
    

	public RssRiverFeedDefinition() {
	}
	
	public RssRiverFeedDefinition(String feedname, String url, int updateRate, boolean ignoreTtl, boolean incrementalDates, String startDate) {
		this.feedname = feedname;
		this.url = url;
		this.updateRate = updateRate;
                this.ignoreTtl = ignoreTtl;
                this.incrementalDates = incrementalDates;
                setStartDate(startDate);
 	}


	public String getFeedname() {
		return feedname;
	}
	
	public void setFeedname(String feedname) {
		this.feedname = feedname;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getUpdateRate() {
		return updateRate;
	}

        public boolean isIncrementalDates() {
            return incrementalDates;
	}
   
        public Date getStartDate() {
		return startDate;
	}

	public void setUpdateRate(int updateRate) {
		this.updateRate = updateRate;
	}

    public boolean isIgnoreTtl() {
        return ignoreTtl;
    }

    public void setIgnoreTtl(boolean ignoreTtl) {
        this.ignoreTtl = ignoreTtl;
    }

       private void setStartDate(String startDate)
        {
            if (startDate == null) {
                Date last3days = new Date();
                last3days.setTime(last3days.getTime() - (3 * 24 * 60 * 60 * 1000));
                this.startDate = last3days;
                return;
            }
            try {
                SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                this.startDate = format.parse(startDate);
            }
            catch(ParseException p0) {
                try {
                    SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    this.startDate = format.parse(startDate);
                }
                catch(ParseException p1) {
                    try {
                        SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
                        this.startDate = format.parse(startDate);
                    }
                    catch(ParseException p2) {
                        try {
                            SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
                            this.startDate = format.parse(startDate);
                        }
                        catch(ParseException p3) {
                            try {
                                SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd");
                                this.startDate = format.parse(startDate);
                            }
                            catch(ParseException p4) {
                            }
                        }
                    }
                }
            }
        }
}
