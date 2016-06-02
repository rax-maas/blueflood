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
package com.rackspacecloud.blueflood.io.astyanax;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.EnumReaderIO;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class holds the utility methods to read/write enum metrics.
 * It's only a wrapper class because we already have a lot of the
 * logic in AstyanaxReader.
 */
public class AEnumIO implements EnumReaderIO {

    private static final Logger LOG = LoggerFactory.getLogger(AEnumIO.class);

    /**
     * Read the metrics_enum column family for the specified locators. Organize
     * the data as a table of locator, enum value hash, and enum value.
     * This is a representation on how the data looks in the column family.
     *
     * @param locators
     * @return
     */
    @Override
    public Table<Locator, Long, String> getEnumHashValuesForLocators(final List<Locator> locators) {
        Map<Locator, ColumnList<Long>> enumHashValueColumnList = getEnumHashMappings(locators);
        Table<Locator, Long, String> enumHashValues = HashBasedTable.create();
        for (Map.Entry<Locator, ColumnList<Long>> locatorToColumnList : enumHashValueColumnList.entrySet() ) {
            Locator locator = locatorToColumnList.getKey();
            ColumnList<Long> columnList = locatorToColumnList.getValue();
            for (Column<Long> column: columnList) {
                enumHashValues.put(locator, column.getName(), column.getStringValue());
            }
        }
        return enumHashValues;
    }

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
    @Override
    public Map<Locator, List<String>> getEnumStringMappings(final List<Locator> locators) {
        return AstyanaxReader.getInstance().getEnumStringMappings(locators);
    }

    /**
     * Read the metrics_preaggregated_{granularity} for specific locators. Organize
     * the data as a table of locator, timestamp, and enum object
     *
     * @param locators
     * @param columnFamily
     * @return
     */
    @Override
    public Table<Locator, Long, BluefloodEnumRollup> getEnumRollupsForLocators(List<Locator> locators,
                                                                               String columnFamily,
                                                                               Range range) {
        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, ColumnList<Long>> locatorToColumnList = reader.getColumnsFromDB(locators, CassandraModel.getColumnFamily(columnFamily), range);
        Table<Locator, Long, BluefloodEnumRollup> locatorHashRollup = HashBasedTable.create();

        for (Map.Entry<Locator, ColumnList<Long>> entry : locatorToColumnList.entrySet()) {
            ColumnList<Long> columnList = entry.getValue();
            for (Column<Long> column: columnList) {
                locatorHashRollup.put(entry.getKey(), column.getName(), column.getValue(Serializers.enumRollupInstance));
            }
        }
        return locatorHashRollup;
    }

    /**
     * This method locates all values of enums from metrics_enum column family by their {@link com.rackspacecloud.blueflood.types.Locator}.
     * The result is organized in a map of Locator -> ColumnList. The ColumnList is a list
     * of columns, each column is a pair of name and value. The name will be the hash
     * value of an enum, and the value would be the string value of the enum.
     *
     * @param locators
     * @return
     */
    private Map<Locator, ColumnList<Long>> getEnumHashMappings(final List<Locator> locators) {

        AstyanaxReader reader = AstyanaxReader.getInstance();
        final Map<Locator, ColumnList<Long>> columns = new HashMap<Locator, ColumnList<Long>>();
        try {
            OperationResult<Rows<Locator, Long>> query = reader.getKeyspace()
                    .prepareQuery(CassandraModel.CF_METRICS_ENUM)
                    .getKeySlice(locators)
                    .execute();

            for (Row<Locator, Long> row : query.getResult()) {
                columns.put(row.getKey(), row.getColumns());
            }
        } catch (ConnectionException e) {
            if (e instanceof NotFoundException) { // TODO: Not really sure what happens when one of the keys is not found.
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_ENUM_NAME);
            } else {
                LOG.warn("Enum read query failed for column family " + CassandraModel.CF_METRICS_ENUM_NAME, e);
                Instrumentation.markReadError(e);
            }
        }
        return columns;
    }
}
