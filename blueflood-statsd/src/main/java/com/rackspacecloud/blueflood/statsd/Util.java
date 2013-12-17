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

package com.rackspacecloud.blueflood.statsd;

public final class Util {
    
    public static Number grokValue(String valuePart) {
        if (valuePart.indexOf(".") < 0)
            return Long.parseLong(valuePart);
        else
            return Double.parseDouble(valuePart);
    }
    
    /** does a left shift of the array */
    public static String[] shiftLeft(String[] s, int num) {
        return Util.shift(s, num, true);
    }
    
    public static String[] shiftRight(String[] s, int num) {
        return Util.shift(s, num, false);
    }
    
    private static String[] shift(String[] s, int num, boolean isLeft) {
        String[] n = new String[s.length - num];
        for (int i = 0; i < s.length - num; i++)
            n[i] = s[i + (isLeft ? num : 0)];
        return n;
    }
    
    public static String[] remove(String[] s, int index, int length) {
        String[] n = new String[s.length - length];
        int npos = 0;
        for (int i = 0; i < s.length; i++) {
            if (i < index)
                n[npos++] = s[i];
            else if (i < index + length)
                continue;
            else
                n[npos++] = s[i];
        }
        return n;
    }
}
