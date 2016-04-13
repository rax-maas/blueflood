/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.types.BluefloodEnumRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;

import java.util.List;
import java.util.Map;

/**
 * An interface describing behavior for reading/writing of Enum values from the
 * metrics_enum and metrics_preaggregated_{granularity} column families.
 *
 * The metrics_enum column family is a table of:
 * Row (key)    | Column1         | Value
 * -------------|-----------------|-------------------
 * metric name  | enum hash code  | enum string value
 *
 * The metrics_preaggregated_{granularity} column family is a table of:
 * Row (key)    | Column1         | Value
 * -------------|-----------------|-------------------
 * metric name  | timestamp       | serialized BluefloodEnumRollup object
 */
public interface EnumReaderIO {

    /**
     * Read the metrics_enum column family for the specified locators. Organize
     * the data as a table of locator, enum value hash, and enum value.
     * This is a representation on how the data looks in the column family.
     * This is to be implemented by driver specific class.
     *
     * @param locators
     * @return
     */
    public Table<Locator, Long, String> getEnumHashValuesForLocators(final List<Locator> locators);

    /**
     * Locates all enum values from metrics_enum column family by their
     * {@link com.rackspacecloud.blueflood.types.Locator}.
     *
     * The result is organized in a map of Locator -> list of String. The string
     * is the string enum values.
     *
     * @param locators
     * @return
     */
    public Map<Locator, List<String>> getEnumStringMappings(final List<Locator> locators);

    /**
     * Read the metrics_preaggregated_{granularity} for specific locators.
     * This is to be implemented by driver specific class.
     *
     * @param locators
     * @param columnFamily
     * @return
     */
    public Table<Locator, Long, BluefloodEnumRollup> getEnumRollupsForLocators(final List<Locator> locators, String columnFamily, Range range);

}
