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

import com.google.common.base.Strings;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

/**
 * This is a poor-man factories for getting the appropriate IO classes.
 */
public class IOFactories {

    // These are numerous IO factories designed to read/write data from/to
    // individual Column Family. Instead of creating factory classes for
    // each one, I elected to take a short-cut and create one for all
    // of them

    public final static String ASTYANAX_DRIVER = "astyanax";

    public final static String DATASTAX_DRIVER = "datastax";

    public final static IOFactories singleton() {
        return INSTANCE;
    }

    private final static IOFactories INSTANCE = new IOFactories();

    private final Configuration configuration = Configuration.getInstance();

    private ShardStateIO shardStateIO;

    /**
     * @return a class for reading/writing ShardState
     */
    public ShardStateIO getShardStateIO() {
        synchronized (IOFactories.class) {
            if ( shouldUseDatastax() ) {
                if (shardStateIO == null || !(shardStateIO instanceof DatastaxShardStateIO)) {
                    shardStateIO = new DatastaxShardStateIO();
                }
            } else {
                if ( shardStateIO == null || ! (shardStateIO instanceof AstyanaxShardStateIO) ) {
                    shardStateIO = new AstyanaxShardStateIO();
                }
            }
        }
        return shardStateIO;
    }

    // prevent ppl from instantiating directly
    private IOFactories() {
    }

    private boolean shouldUseDatastax() {
        // if driver configuration is set to datastax, return true
        // for everything else, return false
        String driver = configuration.getStringProperty(CoreConfig.CASSANDRA_DRIVER);
        return !Strings.isNullOrEmpty(driver) && driver.equalsIgnoreCase(DATASTAX_DRIVER);
    }
}
