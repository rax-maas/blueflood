package com.rackspacecloud.blueflood.io;

import com.google.common.base.Strings;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

/**
 * This is a poor-man factories for getting the appropriate IO classes.

 */
public class IOFactories {

    // There are numerous IO factories design to read/write data from/to
    // individual Column Family. Instead of creating factory classes for
    // each one, I elected to take a short-cut and create one for all
    // of them

    private final static IOFactories INSTANCE = new IOFactories();

    private final Configuration configuration = Configuration.getInstance();

    public final static String ASTYANAX_DRIVER = "astyanax";

    public final static String DATASTAX_DRIVER = "datastax";

    public final static IOFactories singleton() {
        return INSTANCE;
    }

    public ShardStateIO getShardStateIO() {
        String driver = configuration.getStringProperty(CoreConfig.CASSANDRA_DRIVER);
        if (Strings.isNullOrEmpty(driver) || driver.equalsIgnoreCase(ASTYANAX_DRIVER)) {
            return new AstyanaxShardStateIO();
        } else if ( driver.equalsIgnoreCase(DATASTAX_DRIVER) ) {
            return new DatastaxShardStateIO();
        } else {
            throw new IllegalArgumentException(
                            String.format("Invalid Cassandra driver: %s, must be one of %s|%s",
                                          driver, ASTYANAX_DRIVER, DATASTAX_DRIVER));
        }
    }

    // prevent ppl from instantiating directly
    private IOFactories() {
    }

}
