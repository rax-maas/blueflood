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

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.io.astyanax.AEnumIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.datastax.DEnumIO;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests covering EnumIO
 */
public class EnumIOIntegrationTest extends IntegrationTestBase  {

    protected Map<Locator, List<IMetric>> locatorToMetrics;
    protected DEnumIO datastaxEnumIO = new DEnumIO();
    protected AEnumIO astyanaxEnumIO = new AEnumIO();

    /**
     * This method is to supply the granularity parameter to some test methods below
     *
     * @return
     */
    protected Object getGranularitiesToTest() {
        return new Object[] {
                Granularity.FULL,
                Granularity.MIN_5,
                Granularity.MIN_20,
                Granularity.MIN_60,
                Granularity.MIN_240,
                Granularity.MIN_1440
        };
    }


    @Before
    public void generateEnum() throws Exception {
        locatorToMetrics = generateEnumForTenants();
    }

    @RunWith(JUnitParamsRunner.class)
    public static class WriteDatastaxReadAstyanaxReader extends EnumIOIntegrationTest {

        /**
         * Writes enums using Datastax.
         * <p/>
         * Get the locator -> enum string mapping using Astyanax
         * (i.e: call astyanaxReader.getEnumStringMappings()).
         * <p/>
         * Verify that the metrics_enum CF is written properly
         *
         * @throws Exception
         */
        @Test
        public void shouldExistEnumHashToValueMapping() throws Exception {

            // write using Datastax
            for (Map.Entry<Locator, List<IMetric>> entry : locatorToMetrics.entrySet()) {
                Locator locator = entry.getKey();
                List<IMetric> metrics = entry.getValue();
                for (IMetric metric : metrics) {
                    ResultSetFuture future = datastaxEnumIO.putAsync(locator,
                            metric.getCollectionTime(),
                            (BluefloodEnumRollup) metric.getMetricValue(),
                            Granularity.FULL,
                            metric.getTtlInSeconds());
                    future.get().all();
                }
            }

            // read using AstyanaxReader
            AstyanaxReader reader = AstyanaxReader.getInstance();
            final List<Locator> locators = new ArrayList<Locator>() {{
                addAll(locatorToMetrics.keySet());
            }};

            // read the enums from metrics_enum CF, via getEnumStringMappings()
            Map<Locator, List<String>> mapping = reader.getEnumStringMappings(locators);
            assertEquals("locator -> enum string mapping size", locatorToMetrics.size(), mapping.size());

            for (Map.Entry<Locator, List<String>> entry : mapping.entrySet()) {
                List<String> enumValues = entry.getValue();
                assertTrue(String.format("metrics size for locator %s is not bigger than possible enum values defined for these tests", entry.getKey()),
                        enumValues.size() <= enumValueList.size());
            }
        }

        /**
         * Writes enums using Datastax.
         * <p/>
         * Get the locator -> metric data mapping using Astyanax
         * (i.e: call astyanaxReader.getEnumMetricDataForRangeForLocatorList())
         *
         * @throws Exception
         */
        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void shouldExistInMetricsPreaggregatedGran(Granularity granularity) throws Exception {

            // write using Datastax
            for (Map.Entry<Locator, List<IMetric>> entry : locatorToMetrics.entrySet()) {
                Locator locator = entry.getKey();
                List<IMetric> metrics = entry.getValue();
                for (IMetric metric : metrics) {
                    ResultSetFuture future = datastaxEnumIO.putAsync(locator,
                            metric.getCollectionTime(),
                            (BluefloodEnumRollup) metric.getMetricValue(),
                            granularity,
                            metric.getTtlInSeconds());
                    future.get().all();
                }
            }

            // read using Astyanax
            AstyanaxReader reader = AstyanaxReader.getInstance();
            final List<Locator> locators = new ArrayList<Locator>() {{
                addAll(locatorToMetrics.keySet());
            }};

            // ask for 5 minutes back
            Map<Locator, MetricData> results = reader.getEnumMetricDataForRangeForLocatorList(locators,
                                                        getRangeFromMinAgoToNow(5), granularity);
            assertEquals("Results size", locatorToMetrics.size(), results.size());

            for (Locator locator : locators) {
                MetricData metricData = results.get(locator);
                Points points = metricData.getData();

                Set<Map.Entry> entries = points.getPoints().entrySet();
                List<IMetric> expectedMetrics = locatorToMetrics.get(locator);
                assertEquals("metrics size for locator " + locator, expectedMetrics.size(), entries.size());

                for (IMetric metric : expectedMetrics) {
                    Points.Point point = (Points.Point) points.getPoints().get(metric.getCollectionTime());
                    assertNotNull(String.format("point for timestamp %d exists", metric.getCollectionTime()), point);
                }
            }
        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class WriteDatastaxReadAstyanaxEnumIO extends EnumIOIntegrationTest {

        /**
         * Writes enums using Datastax.
         * <p/>
         * Get the locator -> enum string mapping using AstyanaxEnumIO
         * <p/>
         * Verify that metrics_enum hash to string mapping is written properly
         *
         * @throws Exception
         */
        @Test
        public void shouldExistEnumHashToValueMapping() throws Exception {

            // write using Datastax
            for (Map.Entry<Locator, List<IMetric>> entry : locatorToMetrics.entrySet()) {
                Locator locator = entry.getKey();
                List<IMetric> metrics = entry.getValue();
                for (IMetric metric : metrics) {
                    ResultSetFuture future = datastaxEnumIO.putAsync(locator,
                            metric.getCollectionTime(),
                            (BluefloodEnumRollup) metric.getMetricValue(),
                            Granularity.FULL,
                            metric.getTtlInSeconds());
                    future.get().all();
                }
            }

            // read using Astyanax
            final List<Locator> locators = new ArrayList<Locator>() {{
                addAll(locatorToMetrics.keySet());
            }};

            // read the enums from metrics_enum CF, via getEnumStringMappings()
            Table<Locator, Long, String> table = astyanaxEnumIO.getEnumHashValuesForLocators(locators);
            Set<Locator> locatorSet = table.rowKeySet();
            assertEquals("Table(locator, hash, value): # of locators", locatorToMetrics.size(), locatorSet.size());
            for (Map.Entry<Locator, Map<Long, String>> entry : table.rowMap().entrySet()) {
                Locator locator = entry.getKey();
                Map<Long, String> hashValueMap = entry.getValue();
                assertTrue(String.format("metrics size for locator %s is not bigger than possible enum values defined for these tests", locator),
                        hashValueMap.size() <= enumValueList.size());
            }
        }

        /**
         * Writes enums using Datastax.
         * <p/>
         * Get the locator -> enum string mapping using AstyanaxEnumIO
         *
         * @throws Exception
         */
        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void shouldExistInMetricsPreaggregatedGran(Granularity granularity) throws Exception {

            // write using Datastax
            for (Map.Entry<Locator, List<IMetric>> entry : locatorToMetrics.entrySet()) {
                Locator locator = entry.getKey();
                List<IMetric> metrics = entry.getValue();
                for (IMetric metric : metrics) {
                    ResultSetFuture future = datastaxEnumIO.putAsync(locator,
                            metric.getCollectionTime(),
                            (BluefloodEnumRollup) metric.getMetricValue(),
                            granularity,
                            metric.getTtlInSeconds());
                    future.get().all();
                }
            }

            // read using Astyanax
            final List<Locator> locators = new ArrayList<Locator>() {{
                addAll(locatorToMetrics.keySet());
            }};

            // read the enums from metrics_enum CF, via getEnumStringMappings()
            // ask for 5 minutes back
            Table<Locator, Long, BluefloodEnumRollup> table = astyanaxEnumIO.getEnumRollupsForLocators(locators,
                                                                    CassandraModel.getColumnFamily(BluefloodEnumRollup.class, granularity).getName(),
                                                                    getRangeFromMinAgoToNow(5));
            Set<Locator> locatorSet = table.rowKeySet();
            assertEquals("Table(locator, timestamp, rollup): # of locators", locatorToMetrics.size(), locatorSet.size());
            for (Map.Entry<Locator, Map<Long, BluefloodEnumRollup>> entry : table.rowMap().entrySet()) {
                Locator locator = entry.getKey();
                Map<Long, BluefloodEnumRollup> enumRollupMap = entry.getValue();

                // check and make sure all timestamp in our test data is there
                List<IMetric> expectedMetrics = locatorToMetrics.get(locator);
                for (IMetric expected : expectedMetrics) {
                    BluefloodEnumRollup enumRollup = enumRollupMap.get(expected.getCollectionTime());
                    assertNotNull(String.format("enumRollup exists at timestamp %s", expected.getCollectionTime()), enumRollup);
                }
                assertTrue(String.format("metrics size for locator %s is not bigger than possible enum values defined for these tests", locator),
                        enumRollupMap.size() <= enumValueList.size());
            }
        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class WriteAstyanaxReadDatastax extends EnumIOIntegrationTest {

        @Test
        public void shouldExistEnumHashToValueMapping() throws Exception {

            // organize our test data in the way the API wants it
            List<IMetric> metrics = new ArrayList<IMetric>() {{
                for (Locator locator : locatorToMetrics.keySet()) {
                    addAll(locatorToMetrics.get(locator));
                }
            }};

            // write using Astyanax
            AstyanaxWriter writer = AstyanaxWriter.getInstance();
            writer.insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL, false, new DefaultClockImpl());

            // read using Datastax
            final List<Locator> locators = new ArrayList<Locator>() {{
                addAll(locatorToMetrics.keySet());
            }};
            Table<Locator, Long, String> table = datastaxEnumIO.getEnumHashValuesForLocators(locators);
            Set<Locator> locatorSet = table.rowKeySet();
            assertEquals("Table(locator, hash, value): # of locators", locatorToMetrics.size(), locatorSet.size());

            for (Locator locator : locators) {
                Map<Long, String> map = table.row(locator);
                // This test will be run multiple times so it's possible previous
                // runs of this test will have inserted different enum values.
                // Enum values for these tests are randomly generated, see
                // getEnumMetric()
                assertTrue(String.format("metrics size for locator %s is not bigger than possible enum values defined for these tests", locator),
                        map.size() <= enumValueList.size());
            }
        }

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void shouldExistInMetricsPreaggregatedGran(Granularity granularity) throws Exception {

            // organize our test data in the way the API wants it
            List<IMetric> metrics = new ArrayList<IMetric>() {{
                for (Locator locator : locatorToMetrics.keySet()) {
                    addAll(locatorToMetrics.get(locator));
                }
            }};

            // write using Astyanax
            AstyanaxWriter writer = AstyanaxWriter.getInstance();
            writer.insertMetrics(metrics, CassandraModel.getColumnFamily(BluefloodEnumRollup.class, granularity), false, new DefaultClockImpl());

            // read using Datastax, ask for 5 minutes back
            final List<Locator> locators = new ArrayList<Locator>() {{
                addAll(locatorToMetrics.keySet());
            }};
            Table<Locator, Long, BluefloodEnumRollup> results = datastaxEnumIO.getEnumRollupsForLocators(locators,
                    CassandraModel.getColumnFamily(BluefloodEnumRollup.class, granularity).getName(),
                    getRangeFromMinAgoToNow(5));
            assertEquals("Table(locator, timestamp, rollup) size", locatorToMetrics.size(), results.size());

            for (Locator locator : locators) {
                Map<Long, BluefloodEnumRollup> enumRollupMap = results.row(locator);
                // This test will be run multiple times so it's possible previous
                // runs of this test will have inserted different enum values.
                // Enum values for these tests are randomly generated, see
                // getEnumMetric()
                List<IMetric> expectedMetrics = locatorToMetrics.get(locator);
                for (IMetric expected : expectedMetrics) {
                    BluefloodEnumRollup enumRollup = enumRollupMap.get(expected.getCollectionTime());
                    assertNotNull(String.format("enumRollup exists at timestamp %s", expected.getCollectionTime()), enumRollup);
                }
                assertTrue(String.format("metrics size for locator %s is not bigger than possible enum values defined for these tests", locator),
                        expectedMetrics.size() <= enumValueList.size());
            }
        }
    }
}
