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

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeParser {
    public static DateTime parse(String dateTimeOffsetString) {
        String stringToParse = dateTimeOffsetString.replace(" ", "").replace(",", "").replace("_", "");

        if (StringUtils.isNumeric(stringToParse) && !isLikelyDateTime(stringToParse))
            return dateTimeFromTimestamp(stringToParse);

        DateTime dateTime = tryParseDateTime("HH:mmyyyyMMdd", stringToParse);
        if (dateTime != null)
            return dateTime;

        List<String> splitList = splitDateTimeAndOffset(stringToParse);
        String offset = splitList.get(1);
        String dateTimeString = splitList.get(0);

        DateTimeOffsetParser parser = new DateTimeOffsetParser(dateTimeString, offset);
        return parser.updateDateTime(new DateTime());
    }

    private static class DateTimeOffsetParser {
        private String dateTime = "";
        private String offset = "";

        public DateTimeOffsetParser(String dateTimeString, String offsetString) {
            this.dateTime = dateTimeString;
            this.offset = offsetString;
        }

        public DateTime updateDateTime(DateTime baseDateTime) {
            baseDateTime = extractAndUpdateTime(baseDateTime);
            baseDateTime = extractAndUpdateDate(baseDateTime);
            if (!offset.equals(""))
                baseDateTime = updateDateTimeWithOffset(baseDateTime);
            return baseDateTime;
        }

        private DateTime updateDateTimeWithOffset(DateTime baseDateTime) {
            if (offset.equals(""))
                return baseDateTime;
            Pattern p = Pattern.compile("(-?\\d*)([a-z]*)");
            Matcher m = p.matcher(offset);
            if (!m.matches())
                return baseDateTime;

            int count = Integer.parseInt(m.group(1));
            String unit = m.group(2);

            DateTime dateTimeWithOffset = baseDateTime;
            if (unit.startsWith("s"))
                dateTimeWithOffset = baseDateTime.plusSeconds(count);
            else if (unit.startsWith("min"))
                dateTimeWithOffset = baseDateTime.plusMinutes(count);
            else if (unit.startsWith("h"))
                dateTimeWithOffset = baseDateTime.plusHours(count);
            else if (unit.startsWith("d"))
                dateTimeWithOffset = baseDateTime.plusDays(count);
            else if (unit.startsWith("mon"))
                dateTimeWithOffset = baseDateTime.plusMonths(count);
            else if (unit.startsWith("y"))
                dateTimeWithOffset = baseDateTime.plusYears(count);

            return dateTimeWithOffset;
        }

        private DateTime extractAndUpdateTime(DateTime inputDateTime) {
            DateTime resultDateTime = inputDateTime.withSecondOfMinute(0).withMillisOfSecond(0);

            if (dateTime.equals("") || dateTime.contains("now"))
                return resultDateTime;

            int hour = 0;
            int minute = 0;
            Pattern p = Pattern.compile("(\\d{1,2}):(\\d{2})([a|p]m)?(.*)");
            Matcher m = p.matcher(dateTime);
            if (m.matches()) {
                hour = Integer.parseInt(m.group(1));
                minute = Integer.parseInt(m.group(2));

                String middayModifier = m.group(3);
                if (middayModifier != null && middayModifier.equals("pm"))
                    hour = (hour + 12) % 24;

                dateTime = m.group(4);
            }

            if (dateTime.contains("noon")) {
                hour = 12;
                dateTime = dateTime.replace("noon", "");
            }
            else if (dateTime.contains("teatime")) {
                hour = 16;
                dateTime = dateTime.replace("teatime", "");
            } else if (dateTime.contains("midnight"))
                dateTime = dateTime.replace("midnight", "");

            return resultDateTime.withHourOfDay(hour).withMinuteOfHour(minute);
        }

        private DateTime extractAndUpdateDate(DateTime resultDateTime) {
            String stringToParse = this.dateTime;

            if (stringToParse.contains("tomorrow")) {
                resultDateTime = resultDateTime.plusDays(1);
                stringToParse = stringToParse.replace("tomorrow", "");
            } else if (stringToParse.contains("yesterday")) {
                resultDateTime = resultDateTime.minusDays(1);
                stringToParse = stringToParse.replace("yesterday", "");
            } else if (stringToParse.contains("today"))
                stringToParse = stringToParse.replace("today", "");

            String[] datePatterns = {"MM/dd/YY", "MM/dd/YYYY", "YYYYMMdd", "MMMMddYYYY"};
            for (String s : datePatterns) {
                DateTime date = tryParseDateTime(s, stringToParse);
                if (date != null) {
                    resultDateTime = resultDateTime.withDate(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
                    break;
                }
            }

            // Keep original datetime year
            String monthDayOptionalYearFormat = "MMMMdd";
            DateTime date = tryParseDateTime(monthDayOptionalYearFormat, stringToParse);
            if (date != null)
                resultDateTime = resultDateTime.withDate(resultDateTime.getYear(), date.getMonthOfYear(), date.getDayOfMonth());

            // Keep as much of original datetime as possible
            String dayOfWeekFormat = "EEE";
            date = tryParseDateTime(dayOfWeekFormat, stringToParse);
            if (date != null)
                while (resultDateTime.getDayOfWeek() != date.getDayOfWeek())
                    resultDateTime = resultDateTime.minusDays(1);
            return resultDateTime;
        }
    }

    private static List<String> splitDateTimeAndOffset(String stringToSplit) {
        String offset = "";
        String dateTimeString = stringToSplit;
        if (stringToSplit.contains("+")) {
            String[] offsetSplit = stringToSplit.split("\\+", 2);
            dateTimeString = offsetSplit[0];
            offset = offsetSplit.length > 1 ? offsetSplit[1] : "";
        } else if (stringToSplit.contains("-")) {
            String[] offsetSplit = stringToSplit.split("-", 2);
            dateTimeString = offsetSplit[0];
            offset = offsetSplit.length > 1 ? "-" + offsetSplit[1] : "";
        }

        return Arrays.asList(dateTimeString, offset);
    }

    private static DateTime dateTimeFromTimestamp(String stringToParse) {
        return new DateTime(Long.parseLong(stringToParse) * 1000);
    }

    private static DateTime tryParseDateTime(String format, String dateTime) {
        DateTime resultDateTime;
        try {
            resultDateTime = DateTimeFormat.forPattern(format).parseDateTime(dateTime);
        }
        catch (IllegalArgumentException e) {
            resultDateTime = null;
        }
        return resultDateTime;
    }

    private static boolean isLikelyDateTime(String stringToParse) {
        return stringToParse.length() == 8 &&
                Integer.parseInt(stringToParse.substring(0, 4)) > 1900 &&
                Integer.parseInt(stringToParse.substring(4, 6)) < 13 &&
                Integer.parseInt(stringToParse.substring(6)) < 32;
    }
}
