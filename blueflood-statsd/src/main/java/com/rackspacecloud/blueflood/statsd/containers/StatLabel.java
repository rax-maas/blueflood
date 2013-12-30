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

package com.rackspacecloud.blueflood.statsd.containers;

import com.rackspacecloud.blueflood.statsd.StatsdOptions;
import com.rackspacecloud.blueflood.statsd.Util;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.RollupType;

public class StatLabel {
    private final String original;
    private final boolean isInternal;
    private RollupType type;
    private final Locator locator; // might be null!
    private final String name;
    
    private StatLabel(String original, boolean isInternal, RollupType type, Locator locator, String name) {
        this.original = original;
        this.isInternal = isInternal;
        this.type = type;
        this.locator = locator;
        this.name = name;
    }
    
    public void setType(RollupType type) {
        this.type = type;
    }
    
    public static StatLabel parse(String raw, StatsdOptions options) {
        if (options.isLegacyNamespace())
            return LegacyParser.parse(raw, options);
        else
            return Parser.parse(raw, options);
    }

    @Override
    public String toString() {
        return original;
    }

    public RollupType getType() {
        return type;
    }

    public Locator getLocator() {
        return locator;
    }

    public String getName() {
        return name;
    }

    public static class LegacyParser {
        public static StatLabel parse(String raw, StatsdOptions options) {
            String[] parts = raw.split("\\.", -1);
            
            RollupType type;
            Locator locator;
            String name = null;
            boolean internal = false;
            if (parts[0].equals(options.getPrefixStats()) && parts[1].equals("numStats")) {
                internal = true;
                type = RollupType.GAUGE;
                // there will be no global prefix.
            } else if (parts[1].equals(options.getPrefixStats())) {
                internal = true;
                // order in this if statement is important. the second part needs to be checked first.
                if (parts[2].equals("processing_time"))
                    type = RollupType.GAUGE;
                else if (parts[2].equals("graphiteStats"))
                    type = RollupType.GAUGE;
                else if (parts[2].equals("bad_lines_seen") || parts[2].equals("packets_received")) {
                    // rate or count will depend on parts[0].equals(options.getGlobalPrefix())
                    type = RollupType.COUNTER; // bad_lines_seen and packets_received.
                    if (parts[0].equals(options.getGlobalPrefix()))
                        name = "rate";
                    else
                        name = "count";
                } else
                    type = RollupType.BF_BASIC;
                parts = Util.shiftLeft(parts, 1);
            } else {
                // if (type == null) assert internal;
                type = RollupType.BF_BASIC; // will get set later because this is not internal. this satisfies the compiler though.
            }
            
            if (internal) {
                locator = Locator.createLocatorFromPathComponents(options.getInternalTenantId(), parts);
                // type is already set!
            } else {
                boolean isCount = false;
                boolean isRate = false;
                if (parts[0].equals("stats_counts")) {
                    type = RollupType.COUNTER;
                    // after this, tenantId should be exposed.
                    parts = Util.shiftLeft(parts, 1);
                    isCount = true;
                } else if (options.hasGlobalPrefix()) {
                    // get rid of the global prefix.
                    parts = Util.shiftLeft(parts, 1);
                    if (parts[0].equals(options.getPrefixCounter())) {
                        type = RollupType.COUNTER;
                        isCount = true;
                    } else if (parts[0].equals(options.getPrefixGauge()))
                        type = RollupType.GAUGE;
                    else if (parts[0].equals(options.getPrefixSet()))
                        type = RollupType.SET;
                    else if (parts[0].equals(options.getPrefixTimer()))
                        type = RollupType.TIMER;
                    else {
                        // we just shifted of the global prefix on a counter's per_second value. 
                        type = RollupType.COUNTER;
                        // because we're staring at the tenantId.
                        isRate = true;
                    }
                    if (!isRate)
                        parts = Util.shiftLeft(parts, 1);
                } else {
                    // shouldn't happen. there is always a global prefix in legacy mode.
                    type = RollupType.BF_BASIC;
                }
                
                String tenantId = parts[0];
                parts = Util.shiftLeft(parts, 1);
                
                // at this point, I don't care if there is a global suffix. If it was configured to have one, they must
                // want it as part of the identifying locator.
                
                // because timers are split onto multiple lines, the locator comprises every part but the last.
                // name really only matters for timers.
                if (type == RollupType.TIMER) {
                    if (options.hasGlobalSuffix()) {
                        name = parts[parts.length - 2];
                        parts = Util.remove(parts, parts.length - 2, 1);
                        // todo: should the suffix be stripped. that value wasn't on the metric when it came into statsd.
                    } else {
                        name = parts[parts.length - 1];
                        parts = Util.shiftRight(parts, 1);
                    }
                } else if (type == RollupType.COUNTER) {
                    // need to identify count vs rate.
                    if (isCount)
                        name = "count";
                    else if (isRate)
                        name = "rate";
                    else
                        name = null;
                } else {
                    name = null;
                }
                
                locator = Locator.createLocatorFromPathComponents(tenantId, parts);
            }
            
            return new StatLabel(raw, internal, type, locator, name);
        } 
    }
    
    public static class Parser {
        public static StatLabel parse(String raw, StatsdOptions options) {
            
            String[] originalParts = raw.split("\\.", -1);
            String[] parts = originalParts;
            
            if (options.hasGlobalPrefix())
                parts = Util.shiftLeft(parts, 1);
            
            // at this point, we have either type or internal prefix.
            
            boolean internal = false;
            RollupType type;
            Locator locator;
            String tenantId;
            String name = null;
            if (parts[0].startsWith(options.getPrefixStats())) {
                internal = true;
                type = RollupType.GAUGE;
            } else if (parts[0].startsWith(options.getPrefixTimer()) && parts[1].startsWith(options.getPrefixStats())) {
                // bad_lines_seen and packets_received.
                internal = true;
                //  this is internal, so I don't care about destroying parts at this point.
                if (options.hasGlobalSuffix())
                    parts = Util.shiftRight(parts, 1);
                if ("rate".equals(parts[parts.length - 1])) {
                    type = RollupType.COUNTER;
                    name = "rate";
                    parts = Util.shiftRight(parts, 1);
                } else if ("count".equals(parts[parts.length - 1])) {
                    type = RollupType.COUNTER;
                    name = "count";
                    parts = Util.shiftRight(parts, 1);
                } else
                    type = RollupType.BF_BASIC;
            } else {
                // if (type == null) assert internal;
                type = RollupType.BF_BASIC; // will get set when handling non-internals.
            }
            
            if (internal) {
                locator = Locator.createLocatorFromPathComponents(options.getInternalTenantId(), parts);
            } else {
                if (parts[0].equals(options.getPrefixCounter()))
                    type = RollupType.COUNTER;
                else if (parts[0].equals(options.getPrefixTimer()))
                    type = RollupType.TIMER;
                else if (parts[0].equals(options.getPrefixSet()))
                    type = RollupType.SET;
                else if (parts[0].equals(options.getPrefixGauge()))
                    type = RollupType.GAUGE;
                else
                    type = RollupType.BF_BASIC;
                
                tenantId = parts[1];
                parts = Util.shiftLeft(parts, 2); // lops off type and tenant
                
                if (type == RollupType.TIMER || type == RollupType.COUNTER) {
                    if (options.hasGlobalSuffix()) {
                        name = parts[parts.length - 2];
                        parts = Util.remove(parts, parts.length - 2, 1);
                    } else {
                        name = parts[parts.length - 1];
                        parts = Util.shiftRight(parts, 1);
                    }
                } else {
                    name = null;
                }
                
                locator = Locator.createLocatorFromPathComponents(tenantId, parts);
            }
            
            return new StatLabel(raw, internal, type, locator, name);
        }
    }
}
