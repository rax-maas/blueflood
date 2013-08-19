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

package com.rackspacecloud.blueflood.tools.jmx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class OutputFormatter implements Comparable<OutputFormatter> {
    private static final String GAP = "  ";
    
    private final String host;
    private final String[] results;
    
    public OutputFormatter(HostAndPort hostInfo, String[] results) {
        this.host = hostInfo.getHost();
        this.results = results;
    }

    public int compareTo(OutputFormatter o) {
        return host.compareTo(o.host);
    }

    // compute the maximum width for each field across a collection of formatters.
    public static int [] computeMaximums(String[] headers, OutputFormatter... outputs) {
        int[] max = new int[headers.length];
        for (int i = 0; i < headers.length; i++) 
            max[i] = headers[i].length();
        for (OutputFormatter output : outputs) {
            max[0] = Math.max(output.host.length(), max[0]);
            for (int i = 1; i < headers.length; i++)
                max[i] = Math.max(output.results[i-1].length(), max[i]);
        }
        return max;
    }
    
    // formats a header row after maximums have been established.
    public static String formatHeader(int[] maximums, String[] headers) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < headers.length; i++)
            sb = sb.append(formatIn(headers[i], maximums[i], false)).append(GAP);
        return sb.toString();
    }
    
    // formats results and sets formattedStrings.
    public static String[] format(int[] maximums, OutputFormatter... outputs) {
        String[] formattedStrings = new String[outputs.length];
        int pos = 0;
        for (OutputFormatter output : outputs) {
            StringBuilder sb = new StringBuilder();
            sb = sb.append(formatIn(output.host, maximums[0], false));
            for (int i = 0; i < output.results.length; i++)
                sb = sb.append(GAP).append(formatIn(output.results[i], maximums[i+1], true));
            formattedStrings[pos++] = sb.toString();
        }
        return formattedStrings;
    }
    
    private static String formatIn(String s, int spaces, boolean rightAlign) {
        while (s.length() < spaces) {
            if (rightAlign)
                s = " " + s;
            else
                s += " ";
        }
        return s;
    }
    
    public static Collection<OutputFormatter> sort(Collection<OutputFormatter> src) {
        List<OutputFormatter> sortedList = new ArrayList<OutputFormatter>(src);
        Collections.sort(sortedList);
        return sortedList;
    }
    
}
