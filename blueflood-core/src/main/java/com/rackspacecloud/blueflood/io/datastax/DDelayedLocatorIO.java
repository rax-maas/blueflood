package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.rackspacecloud.blueflood.cache.TenantTtlProvider;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.DelayedLocatorIO;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class DDelayedLocatorIO implements DelayedLocatorIO {

    private static final Logger LOG = LoggerFactory.getLogger(DDelayedLocatorIO.class);

    private static final String KEY = "key";
    private static final String COLUMN1 = "column1";
    private static final String VALUE = "value";

    private final PreparedStatement getValue;
    private final PreparedStatement putValue;

    public DDelayedLocatorIO() {

        // create a generic select statement for retrieving from metrics_delayed_locator
        Select.Where select = QueryBuilder
                .select()
                .all()
                .from( CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME )
                .where( eq ( KEY, bindMarker() ));
        getValue = DatastaxIO.getSession().prepare( select );

        // create a generic insert statement for inserting into metrics_delayed_locator
        Insert insert = QueryBuilder.insertInto( CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME)
                .using(ttl(TenantTtlProvider.DELAYED_LOCATOR_TTL))
                .value(KEY, bindMarker())
                .value(COLUMN1, bindMarker())
                .value(VALUE, bindMarker());
        putValue = DatastaxIO.getSession()
                .prepare(insert)
                .setConsistencyLevel( ConsistencyLevel.ONE );  // TODO: remove later; required by the cassandra-maven-plugin 2.0.0-1
        // (see https://issues.apache.org/jira/browse/CASSANDRA-6238)
    }


    @Override
    public void insertLocator(Granularity g, int slot, Locator locator) throws IOException {
        int shard = Util.getShard(locator.toString());

        Session session = DatastaxIO.getSession();

        Timer.Context timer = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME);
        try {
            // bound values and execute
            BoundStatement bs = putValue.bind(SlotKey.of(g, slot, shard).toString(), locator.toString(), "");
            session.execute(bs);
        } finally {
            timer.stop();
        }
    }

    @Override
    public Collection<Locator> getLocators(SlotKey slotKey) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME);
        Session session = DatastaxIO.getSession();

        Collection<Locator> locators = new ArrayList<Locator>();

        try {
            // bind value
            BoundStatement bs = getValue.bind(slotKey.toString());
            List<Row> results = session.execute(bs).all();
            for ( Row row : results ) {
                locators.add(Locator.createLocatorFromDbKey(row.getString(COLUMN1)));
            }

            // return results
            if (locators.size() == 0) {
                Instrumentation.markNotFound(CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME);
                return Collections.emptySet();
            }
            else {
                return locators;
            }
        } finally {
            ctx.stop();
        }
    }
}
