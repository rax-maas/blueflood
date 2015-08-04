/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.utils.DateTimeParser;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.Arrays;

public class DateTimeParserTest {
    @Test
    public void testFromUnixTimestamp() {
        long unixTimestamp = nowDateTime().getMillis() / 1000;

        Assert.assertEquals(DateTimeParser.parse(Long.toString(unixTimestamp)),
                nowDateTime());
    }

    @Test
    public void testPlainTimeDateFormat() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mmyyyyMMdd");
        String dateTimeWithSpace = "10:55 2014 12 20";
        String dateTimeWithUnderscore = "10:55_2014_12_20";

        Assert.assertEquals(DateTimeParser.parse(dateTimeWithSpace),
                new DateTime(formatter.parseDateTime(dateTimeWithSpace.replace(" ", ""))));

        Assert.assertEquals(DateTimeParser.parse(dateTimeWithUnderscore),
                new DateTime(formatter.parseDateTime(dateTimeWithUnderscore.replace("_", ""))));
    }

    @Test
    public void testNowKeyword() {
        String nowTimestamp = "now";

        Assert.assertEquals(DateTimeParser.parse(nowTimestamp),
                nowDateTime());
    }

    @Test
    public void testRegularHourMinute() {
        String hourMinuteTimestamp = "12:24";
        String hourMinuteWithAm = "9:13am";
        String hourMinuteWithPm = "09:13pm";

        Assert.assertEquals(DateTimeParser.parse(hourMinuteTimestamp),
                referenceDateTime().withHourOfDay(12).withMinuteOfHour(24));

        Assert.assertEquals(DateTimeParser.parse(hourMinuteWithAm),
                referenceDateTime().withHourOfDay(9).withMinuteOfHour(13));

        Assert.assertEquals(DateTimeParser.parse(hourMinuteWithPm),
                referenceDateTime().withHourOfDay(21).withMinuteOfHour(13));
    }

    @Test
    public void testHourMinuteKeywords() {
        String noonTimestamp = "noon";
        String teatimeTimestamp = "teatime";
        String midnightTimestamp = "midnight";

        Assert.assertEquals(DateTimeParser.parse(noonTimestamp),
                referenceDateTime().withHourOfDay(12).withMinuteOfHour(0));

        Assert.assertEquals(DateTimeParser.parse(teatimeTimestamp),
                referenceDateTime().withHourOfDay(16).withMinuteOfHour(0));

        Assert.assertEquals(DateTimeParser.parse(midnightTimestamp),
                referenceDateTime().withHourOfDay(0).withMinuteOfHour(0));
    }

    @Test
    public void testDayKeywords() {
        String todayTimestamp = "today";
        String yesterdayTimestamp = "yesterday";
        String tomorrowTimeStamp = "tomorrow";

        Assert.assertEquals(DateTimeParser.parse(todayTimestamp),
                referenceDateTime());

        Assert.assertEquals(DateTimeParser.parse(yesterdayTimestamp),
                referenceDateTime().minusDays(1));

        Assert.assertEquals(DateTimeParser.parse(tomorrowTimeStamp),
                referenceDateTime().plusDays(1));
    }

    @Test
    public void testDateFormats() {
        int currentYear = referenceDateTime().getYear();
        testFormat("12/30/14", new DateTime(2014, 12, 30, 0, 0, 0, 0));
        testFormat("12/30/2014", new DateTime(2014, 12, 30, 0, 0, 0, 0));
        testFormat("Jul 30", new DateTime(currentYear, 07, 30, 0, 0, 0, 0));
        testFormat("Jul 30, 2013", new DateTime(2013, 07, 30, 0, 0, 0, 0));
        testFormat("20141230", new DateTime(2014, 12, 30, 0, 0, 0, 0));
    }

    @Test
    public void testDayOfWeekFormat() {
        DateTime todayDate = referenceDateTime();
        for (String dateTimeString: Arrays.asList("Sun", "14:42 Sun", "noon Sun")) {
            DateTime date = DateTimeParser.parse(dateTimeString);
            Assert.assertEquals(date.getDayOfWeek(), 7);
            Assert.assertTrue(todayDate.getYear() == date.getYear());
            Assert.assertTrue(todayDate.getDayOfYear() - date.getDayOfYear() <= 7);
        }
    }

    @Test
    public void testIncrementDecrement() {
        testFormat("now-10h", nowDateTime().minusHours(10));
        testFormat("now+10h", nowDateTime().plusHours(10));
    }

    @Test
    public void testDecrementUnits() {
        testFormat("now-10s", nowDateTime().minusSeconds(10));
        testFormat("now-15min", nowDateTime().minusMinutes(15));
        testFormat("now-100h", nowDateTime().minusHours(100));
        testFormat("now-2d", nowDateTime().minusDays(2));
        testFormat("now-6mon", nowDateTime().minusMonths(6));
        testFormat("now-5y", nowDateTime().minusYears(5));
        testFormat("-6h", nowDateTime().minusHours(6));
    }

    @Test
    public void testIncrementUnits() {
        testFormat("now+10s", nowDateTime().plusSeconds(10));
        testFormat("now+15min", nowDateTime().plusMinutes(15));
        testFormat("now+100h", nowDateTime().plusHours(100));
        testFormat("now+2d", nowDateTime().plusDays(2));
        testFormat("now+6mon", nowDateTime().plusMonths(6));
        testFormat("now+5y", nowDateTime().plusYears(5));
    }

    @Test
    public void testComplexFormats() {
        testFormat("12:24 yesterday", nowDateTime().minusDays(1).withHourOfDay(12).withMinuteOfHour(24));
        testFormat("12:24 tomorrow", nowDateTime().plusDays(1).withHourOfDay(12).withMinuteOfHour(24));
        testFormat("12:24 today", nowDateTime().withHourOfDay(12).withMinuteOfHour(24));
        testFormat("noon 12/30/2014", nowDateTime().withDate(2014, 12, 30).withHourOfDay(12).withMinuteOfHour(0));

        int currentYear = referenceDateTime().getYear();
        testFormat("15:45 12/30/14", new DateTime(2014, 12, 30, 15, 45, 0, 0));
        testFormat("teatime 12/30/2014", new DateTime(2014, 12, 30, 16, 0, 0, 0));
        testFormat("midnight Jul 30", new DateTime(currentYear, 07, 30, 0, 0, 0, 0));
        testFormat("Jul 30, 2013", new DateTime(2013, 07, 30, 0, 0, 0, 0));
        testFormat("Jul 30", new DateTime(currentYear, 07, 30, 0, 0, 0, 0));
        testFormat("20141230", new DateTime(2014, 12, 30, 0, 0, 0, 0));
    }

    private void testFormat(String dateString, DateTime date) {
        Assert.assertEquals(DateTimeParser.parse(dateString), date);
    }

    private static DateTime referenceDateTime() {
        return new DateTime().withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
    }

    private static DateTime nowDateTime() {
        return new DateTime().withSecondOfMinute(0).withMillisOfSecond(0);
    }

}
