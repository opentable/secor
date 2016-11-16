/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AnalyticsMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * usually named @timestamp in logstash.
 * It uses the ISODateTimeFormat from joda-time library. Used by elasticsearch / logstash
 *
 * <p>
 * If the JSON object message has a platform string field under the properties object field equal to "SIMULATION",
 * {@link #parse(Message)} will return {@code null}.
 *
 * @see http://joda-time.sourceforge.net/apidocs/org/joda/time/format/ISODateTimeFormat.html
 *
 * @author Pablo Delgado (pablete@gmail.com)
 *
 */
public class AnalyticsMessageParser extends MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsMessageParser.class);
    protected static final String defaultType = "untyped";
    protected static final String defaultDate = "1970/01/01/00";

    private static final Duration FILTER_LOG_THRESH = Duration.standardMinutes(10);
    // Start out last timestamp so that we're guaranteed to log the first time.
    private Instant lastFilterLog = Instant.now().minus(FILTER_LOG_THRESH).minus(FILTER_LOG_THRESH);
    private long filterCount = 0;

    @VisibleForTesting
    JSONObject jsonObject;

    public AnalyticsMessageParser(SecorConfig config) {
        super(config);
    }

    /**
     * Because this method depends on the {@link #jsonObject} state, it is not reentrant.
     * However, based on {@link com.pinterest.secor.consumer.Consumer usage} in this codebase,
     * it does not seem like that will be a problem.
     */
    @Override
    public ParsedMessage parse(final Message message) throws Exception {
        jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        try {
            if (shouldFilter()) {
                return null;
            }
            return super.parse(message);
        } finally {
            jsonObject = null;
        }
    }

    /**
     * Note that this depends on the {@link #jsonObject} state.
     */
    @Override
    public String[] extractPartitions(Message message) {
        String result[] = {defaultType, defaultDate};
        String event_type = "";
        String analytics_type = "";

        /**
         * The idea is to store a directory structure that looks like
         * analytics-bucket/27/identify/2015/05/19/27/xxxxxxxxxxxx.json
         *
         *                  ^-- repeat the hour to spread the load among 24 shards
         *
         * analytics payloads may be of type: "track", event:"user_definded_name"
         *                           or type: "identify"
         *                           or type: "page"
         *                           or type: "screen"
         */

        if (jsonObject != null) {
            Object fieldType  = jsonObject.get(mConfig.getMessageTypeName());       //type
            Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());  //timestamp

            if (fieldType != null) {
                analytics_type = fieldType.toString();
                if (analytics_type.equals("track")) {
                    Object fieldSecondary = jsonObject.get("event");
                    event_type = sanitizePath(fieldSecondary.toString());
                } else {
                    event_type = analytics_type;
                }
            }

            if (fieldValue != null) {
                try {
                    DateTimeFormatter inputFormatter = ISODateTimeFormat.dateOptionalTimeParser();
                    LocalDateTime datetime = LocalDateTime.parse(fieldValue.toString(), inputFormatter);
                    result[1] = datetime.toString(mConfig.getMessageTimestampBucketFormat());
                } catch (Exception e) {
                    LOG.warn("date = " + fieldValue.toString()
                            + " could not be parsed with ISODateTimeFormat."
                            + " Using date default=" + defaultDate);
                }
            }
        }

        // The hour bucket where the event happened
        String hour = result[1].split("/")[3];
        result[0] = hour +"/" + event_type;

        return result;
    }

    private String sanitizePath(String path_type) {
      //Accept only lowercase underscores and hypens
      return path_type.replaceAll("\\.","-").replaceAll("[^a-zA-Z0-9-_]", "").replaceAll("---","-").replaceAll("--","-").replaceAll("___","_").replaceAll("__","_").toLowerCase();
    }

    /**
     * Note that this depends on the {@link #jsonObject} state.
     */
    private boolean shouldFilter() {
        if (jsonObject == null) {
            return false;
        }
        if (!jsonObject.containsKey("properties")) {
            return false;
        }
        final Object propertiesObject = jsonObject.get("properties");
        if (!(propertiesObject instanceof JSONObject)) {
            return false;
        }
        final JSONObject properties = (JSONObject) propertiesObject;
        if (!properties.containsKey("platform")) {
            return false;
        }
        final Object platformObject = properties.get("platform");
        if (!(platformObject instanceof String)) {
            return false;
        }
        final String platform = (String) platformObject;
        final boolean ret = "SIMULATION".equals(platform);
        if (ret) {
            ++filterCount;
            final Instant now = Instant.now();
            if (now.isAfter(lastFilterLog.plus(FILTER_LOG_THRESH))) {
                LOG.info("filtered platform simulation messages: {}", filterCount);
                lastFilterLog = now;
            }
        }
        return ret;
    }

}
