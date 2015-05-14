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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
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
 * @see http://joda-time.sourceforge.net/apidocs/org/joda/time/format/ISODateTimeFormat.html
 *
 * @author Pablo Delgado (pablete@gmail.com)
 *
 */
public class AnalyticsMessageParser extends MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsMessageParser.class);
    protected static final String defaultType = "untyped";
    protected static final String defaultDate = "1970/01/01/00";

    public AnalyticsMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public String[] extractPartitions(Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        String result[] = {defaultType, defaultDate};
        String analytics_type = "";

        /**
         * The idea is to store a directory structure that looks like
         * analytics/identify/2015/05/19/22/xxxxxxxxxxxx.json
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
                    result[0] = sanitizePath(fieldSecondary.toString());
                } else {
                    result[0] = analytics_type;
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

        return result;
    }

    private String sanitizePath(String path_type) {
      //Accept only lowercase underscores and hypens
      return path_type.replaceAll("\\.","-").replaceAll("[^a-zA-Z0-9-_]", "").replaceAll("---","-").replaceAll("--","-").replaceAll("___","_").replaceAll("__","_").toLowerCase();
    }

}
