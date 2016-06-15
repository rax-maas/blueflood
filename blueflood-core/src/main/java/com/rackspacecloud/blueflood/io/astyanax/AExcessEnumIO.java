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

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.ExcessEnumIO;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * This class uses the Astyanax driver to read/write excess enum metrics from
 * Cassandra metrics_excess_enums Column Family.
 */
public class AExcessEnumIO implements ExcessEnumIO {

    private static final Logger LOG = LoggerFactory.getLogger(AExcessEnumIO.class);

    @Override
    public Set<Locator> getExcessEnumMetrics() throws IOException {

        final Set<Locator> excessEnumMetrics = new HashSet<Locator>();
        Function<Row<Locator, Long>, Boolean> rowFunction = new Function<Row<Locator, Long>, Boolean>() {
            @Override
            public Boolean apply(Row<Locator, Long> row) {
                excessEnumMetrics.add(row.getKey());
                return true;
            }
        };

        ColumnFamily CF = CassandraModel.CF_METRICS_EXCESS_ENUMS;
        Timer.Context ctx = Instrumentation.getBatchReadTimerContext(CF.getName());
        try {
            // Get all the Row Keys
            new AllRowsReader.Builder<Locator, Long>(AstyanaxIO.getKeyspace(), CassandraModel.CF_METRICS_EXCESS_ENUMS)
                    .withColumnRange(null, null, false, 0)
                    .forEachRow(rowFunction)
                    .build()
                    .call();
        } catch (ConnectionException e) {
            LOG.error("Error with connection to ExcessEnum Metrics Table", e);
            Instrumentation.markReadError(e);
            Instrumentation.markExcessEnumReadError();
            throw new IOException(e);
        }
        catch (Exception ex) {
            LOG.error("Error reading ExcessEnum Metrics Table", ex);
            Instrumentation.markExcessEnumReadError();
            throw new IOException(ex);
        }
        finally {
            ctx.stop();
        }

        return excessEnumMetrics;
    }

    @Override
    public void insertExcessEnumMetric(Locator locator) throws IOException {

        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME);
        try {
            AstyanaxIO.getKeyspace().prepareColumnMutation(CassandraModel.CF_METRICS_EXCESS_ENUMS, locator, 0L)
                    .putEmptyColumn(null).execute();
        } catch (ConnectionException e) {
            Instrumentation.markWriteError(e);
            Instrumentation.markExcessEnumWriteError();
            LOG.error("Error writing ExcessEnum Metric " + locator, e);
            throw new IOException(e);
        } finally {
            ctx.stop();
        }

    }

}
