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

package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;

import java.util.HashMap;
import java.util.Map;

/**
 * A class that holds together all the Datastax PreparedStatement related to
 * inserting/querying ColumnFamilies used to store the actual metrics
 * (i.e: metrics_full, metrics_{gran}, metrics_preaggregated_full, and
 * metrics_preaggregated_{gran})
 *
 * This is a singleton to ensure that the PreparedStatements are created
 * only once.
 */
public class DMetricsCFPreparedStatements {

    protected static final String INSERT_KEY_COLUMN_VALUE_TTL_FORMAT = "INSERT INTO %s (key, column1, value) VALUES (?, ?, ?) USING TTL ?";
    protected static final String SELECT_FOR_KEY_RANGE_FORMAT = "SELECT * FROM %s WHERE key = :locator AND column1 >= :tsStart AND column1 <= :tsEnd";

    /**
     * The key of the metrics_* and metrics_preaggregated_* Column Families
     */
    public static final String KEY = "key";

    /**
     * The name of the first column
     */
    public static final String COLUMN1 = "column1";

    /**
     * The name of the value column
     */
    public static final String VALUE = "value";

    private static final DMetricsCFPreparedStatements INSTANCE = new DMetricsCFPreparedStatements();

    protected final PreparedStatement insertToMetricsPreaggrFullStatement;
    protected final PreparedStatement insertToMetricsPreaggr5MStatement;
    protected final PreparedStatement insertToMetricsPreaggr20MStatement;
    protected final PreparedStatement insertToMetricsPreaggr60MStatement;
    protected final PreparedStatement insertToMetricsPreaggr240MStatement;
    protected final PreparedStatement insertToMetricsPreaggr1440MStatement;

    protected final PreparedStatement selectFromMetricsPreaggrFullForRangeStatement;
    protected final PreparedStatement selectFromMetricsPreaggr5MForRangeStatement;
    protected final PreparedStatement selectFromMetricsPreaggr20MForRangeStatement;
    protected final PreparedStatement selectFromMetricsPreaggr60MForRangeStatement;
    protected final PreparedStatement selectFromMetricsPreaggr240MForRangeStatement;
    protected final PreparedStatement selectFromMetricsPreaggr1440MForRangeStatement;

    protected final PreparedStatement insertToMetricsBasicFullStatement;
    protected final PreparedStatement insertToMetricsBasic5MStatement;
    protected final PreparedStatement insertToMetricsBasic20MStatement;
    protected final PreparedStatement insertToMetricsBasic60MStatement;
    protected final PreparedStatement insertToMetricsBasic240MStatement;
    protected final PreparedStatement insertToMetricsBasic1440MStatement;

    protected final PreparedStatement selectFromMetricsStringForRangeStatement;

    protected final PreparedStatement selectFromMetricsBasicFullForRangeStatement;
    protected final PreparedStatement selectFromMetricsBasic5MForRangeStatement;
    protected final PreparedStatement selectFromMetricsBasic20MForRangeStatement;
    protected final PreparedStatement selectFromMetricsBasic60MForRangeStatement;
    protected final PreparedStatement selectFromMetricsBasic240MForRangeStatement;
    protected final PreparedStatement selectFromMetricsBasic1440MForRangeStatement;

    protected Map<String, PreparedStatement> cfNameToSelectStatement;
    protected Map<Granularity, PreparedStatement> preaggrGranToInsertStatement;
    protected Map<Granularity, PreparedStatement> basicGranToInsertStatement;

    private DMetricsCFPreparedStatements() {
        Session session = DatastaxIO.getSession();

        //
        // Preaggr insert statements
        //
        // TODO: The call to setConsistency(ONE) is required by
        // the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
        // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
        insertToMetricsPreaggrFullStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsPreaggr5MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsPreaggr20MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsPreaggr60MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsPreaggr240MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsPreaggr1440MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin

        //
        // Preaggr select statements
        //
        selectFromMetricsPreaggrFullForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME));
        selectFromMetricsPreaggr5MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME));
        selectFromMetricsPreaggr20MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME));
        selectFromMetricsPreaggr60MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME));
        selectFromMetricsPreaggr240MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME));
        selectFromMetricsPreaggr1440MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME));

        cfNameToSelectStatement = new HashMap<String, PreparedStatement>() {{
            put(CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME, selectFromMetricsPreaggrFullForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME, selectFromMetricsPreaggr5MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_20M_NAME, selectFromMetricsPreaggr20MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_60M_NAME, selectFromMetricsPreaggr60MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_240M_NAME, selectFromMetricsPreaggr240MForRangeStatement);
            put(CassandraModel.CF_METRICS_PREAGGREGATED_1440M_NAME, selectFromMetricsPreaggr1440MForRangeStatement);
        }};

        preaggrGranToInsertStatement = new HashMap<Granularity, PreparedStatement>() {{
            put(Granularity.FULL, insertToMetricsPreaggrFullStatement);
            put(Granularity.MIN_5, insertToMetricsPreaggr5MStatement);
            put(Granularity.MIN_20, insertToMetricsPreaggr20MStatement);
            put(Granularity.MIN_60, insertToMetricsPreaggr60MStatement);
            put(Granularity.MIN_240, insertToMetricsPreaggr240MStatement);
            put(Granularity.MIN_1440, insertToMetricsPreaggr1440MStatement);
        }};

        //
        // Basic insert statements
        //
        // TODO: The call to setConsistency(ONE) is required by
        // the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
        // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
        insertToMetricsBasicFullStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_FULL_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsBasic5MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_5M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsBasic20MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_20M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsBasic60MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_60M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsBasic240MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_240M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin
        insertToMetricsBasic1440MStatement = session.prepare(
                String.format(INSERT_KEY_COLUMN_VALUE_TTL_FORMAT,
                        CassandraModel.CF_METRICS_1440M_NAME))
                .setConsistencyLevel(ConsistencyLevel.ONE); // needed by maven cassandra plugin

        //
        // Basic select statements
        //
        selectFromMetricsStringForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_STRING_NAME));
        selectFromMetricsBasicFullForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_FULL_NAME));
        selectFromMetricsBasic5MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_5M_NAME));
        selectFromMetricsBasic20MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_20M_NAME));
        selectFromMetricsBasic60MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_60M_NAME));
        selectFromMetricsBasic240MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_240M_NAME));
        selectFromMetricsBasic1440MForRangeStatement = session.prepare(
                String.format(SELECT_FOR_KEY_RANGE_FORMAT,
                        CassandraModel.CF_METRICS_1440M_NAME));

        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_STRING_NAME, selectFromMetricsStringForRangeStatement );

        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_FULL_NAME, selectFromMetricsBasicFullForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_5M_NAME, selectFromMetricsBasic5MForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_20M_NAME, selectFromMetricsBasic20MForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_60M_NAME, selectFromMetricsBasic60MForRangeStatement );
        cfNameToSelectStatement.put( CassandraModel.CF_METRICS_240M_NAME, selectFromMetricsBasic240MForRangeStatement );
        cfNameToSelectStatement.put(CassandraModel.CF_METRICS_1440M_NAME, selectFromMetricsBasic1440MForRangeStatement);

        basicGranToInsertStatement = new HashMap<Granularity, PreparedStatement>() {{
            // NOTE:  this shoudn't be called.  explain why later
            put(Granularity.FULL, insertToMetricsBasicFullStatement );
            put(Granularity.MIN_5, insertToMetricsBasic5MStatement);
            put(Granularity.MIN_20, insertToMetricsBasic20MStatement);
            put(Granularity.MIN_60, insertToMetricsBasic60MStatement);
            put(Granularity.MIN_240, insertToMetricsBasic240MStatement);
            put(Granularity.MIN_1440, insertToMetricsBasic1440MStatement);
        }};
    }

    /**
     * Method to fetch the singleton instance
     * @return
     */
    public static DMetricsCFPreparedStatements getInstance() { return INSTANCE; }
}
