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
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.LocatorIO;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * This class uses the Astyanax driver to read/write locators from
 * Cassandra metrics_locator Column Family.
 */
public class AstyanaxLocatorIO implements LocatorIO {

    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxLocatorIO.class);

    /**
     * Insert a locator with key = shard long value calculated using Util.getShard()
     * @param locator
     * @throws IOException
     */
    @Override
    public void insertLocator(Locator locator) throws IOException {
        try {
            MutationBatch mutationBatch = AstyanaxIO.getKeyspace().prepareMutationBatch();
            AstyanaxWriter.getInstance().insertLocator(locator, mutationBatch);
            mutationBatch.execute();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns the locators for a shard, i.e. those that should be rolled up, for a given shard.
     * 'Should' means:
     *  1) A locator is capable of rollup (it is not a string/boolean metric).
     *  2) A locator has had new data in the past LOCATOR_TTL seconds.
     *
     * @param shard Number of the shard you want the locators for. 0-127 inclusive.
     * @return Collection of locators
     * @throws IOException
     */
    @Override
    public Collection<Locator> getLocators(long shard) throws IOException {
        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_LOCATOR_NAME);
        final Collection<Locator> locators = new ArrayList<Locator>();
        try {
            RowQuery<Long, Locator> query = AstyanaxIO.getKeyspace()
                    .prepareQuery(CassandraModel.CF_METRICS_LOCATOR)
                    .getKey(shard);

            ColumnList<Locator> columns = query.execute().getResult();
            for (Column<Locator> column: columns) {
                locators.add(column.getName()
                        .withLastUpdatedTimestamp(TimeUnit.MILLISECONDS.convert(column.getTimestamp(), TimeUnit.MICROSECONDS)));
            }

            if (locators.size() == 0) {
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_LOCATOR_NAME);
                return Collections.emptySet();
            }
        } catch (NotFoundException e) {
            Instrumentation.markNotFound(CassandraModel.CF_METRICS_LOCATOR_NAME);
            return Collections.emptySet();
        } catch (ConnectionException ex) {
            Instrumentation.markReadError(ex);
            LOG.error("Connection exception during getLocators(" + Long.toString(shard) + " )", ex);
            throw new IOException("Error reading locators", ex);
        } finally {
            ctx.stop();
        }

        return locators;
    }

}
