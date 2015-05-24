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

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

@RunWith(PowerMockRunner.class)
public class AnalyticsMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mTypeTrack;
    private Message mTypeIdentify;
    private Message mInvalidDate;
    private Message mInvalidPath;
    private Message mRobustInvalidPath;
    private Message mDateWithoutMilliseconds;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageTypeName()).thenReturn("type");
        Mockito.when(mConfig.getMessageTimestampBucketFormat()).thenReturn("yyyy/MM/dd/HH");
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");

        byte type_track[] = "{\"timestamp\":\"2014-10-17T01:34:22.450+00:00\",\"type\":\"track\",\"event\":\"availability\"}".getBytes("UTF-8");
        mTypeTrack = new Message("test", 0, 0, type_track);

        byte type_identify[] = "{\"timestamp\":\"2014-10-17T13:34:22.450+00:00\",\"type\":\"identify\"}".getBytes("UTF-8");
        mTypeIdentify = new Message("test", 0, 0, type_identify);

        byte invalid_date[] = "{\"timestamp\":\"222222222222\",\"type\":\"track\",\"event\":\"availability\"}".getBytes("UTF-8");
        mInvalidDate = new Message("test", 0, 0, invalid_date);

        byte invalid_path[] = "{\"timestamp\":\"2014-10-17T01:34:22.450+00:00\",\"type\":\"track\",\"event\":\"Task Scheduler - Task Published Event   - V 1.0\"}".getBytes("UTF-8");
        mInvalidPath = new Message("test", 0, 0, invalid_path);

        byte robuts_invalid_path[] = "{\"timestamp\":\"2014-10-17T01:34:22.450+00:00\",\"type\":\"track\",\"event\":\"search:*results-'|dir-v1\"}".getBytes("UTF-8");
        mRobustInvalidPath = new Message("test", 0, 0, robuts_invalid_path);

        byte date_without_milliseconds[] = "{\"timestamp\":\"2015-01-27T18:31:01Z\",\"type\":\"track\",\"event\":\"sometype-v1\"}".getBytes("UTF-8");
        mDateWithoutMilliseconds = new Message("test", 0, 0, date_without_milliseconds);

    }

    @Test
    public void testExtractTypeAndDate() throws Exception {
        String result[] = new AnalyticsMessageParser(mConfig).extractPartitions(mTypeTrack);
        assertEquals("01/availability", result[0]);
        assertEquals("2014/10/17/01", result[1]);
    }

    public void testExtractTypeAndDate2() throws Exception {
        String result[] = new AnalyticsMessageParser(mConfig).extractPartitions(mTypeIdentify);
        assertEquals("13/identify", result[0]);
        assertEquals("2014/10/17/13", result[1]);
    }

    public void testExtractTypeAndInvalidDate() throws Exception {
        String result[] = new AnalyticsMessageParser(mConfig).extractPartitions(mInvalidDate);
        assertEquals("00/availability", result[0]);
        assertEquals("1970/01/01/00", result[1]);
    }

    public void testSanitizePath() throws Exception {
        String result[] = new AnalyticsMessageParser(mConfig).extractPartitions(mInvalidPath);
        assertEquals("01/taskscheduler-taskpublishedevent-v1-0", result[0]);
        assertEquals("2014/10/17/01", result[1]);
    }

    public void testSanitizePathRobust() throws Exception {
        String result[] = new AnalyticsMessageParser(mConfig).extractPartitions(mRobustInvalidPath);
        assertEquals("01/searchresults-dir-v1", result[0]);
        assertEquals("2014/10/17/01", result[1]);
    }

    public void testDateWithoutMilliseconds() throws Exception {
        String result[] = new AnalyticsMessageParser(mConfig).extractPartitions(mDateWithoutMilliseconds);
        assertEquals("2015/01/27/18", result[1]);
    }

}
