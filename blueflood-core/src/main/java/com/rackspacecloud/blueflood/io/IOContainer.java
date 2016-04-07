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
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxMetadataIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxMetadataIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

/**
 * This class holds the necessary IO classes for a particular driver type.
 */
public class IOContainer {

    // These are numerous IO factories designed to read/write data from/to
    // individual Column Family. Instead of creating factory classes for
    // each one, I elected to take a short-cut and create one for all
    // of them

    private static final Configuration configuration = Configuration.getInstance();

    private ShardStateIO shardStateIO;
    private MetadataIO metadataIO;

    // more IO classes to follow

    /**
     * Creates an instance of this class based on what configuration says our
     * driver should be.
     *
     * @return IOContainer
     */
    public static IOContainer fromConfig() {
        String driver = configuration.getStringProperty(CoreConfig.CASSANDRA_DRIVER);
        return new IOContainer(DriverType.getDriverType(driver));
    }

    /**
     * Construct an instance of this class based on the specified
     * {@link com.rackspacecloud.blueflood.io.IOContainer.DriverType}
     *
     * @param driver
     */
    public IOContainer(DriverType driver) {

        if ( driver == DriverType.DATASTAX ) {

            shardStateIO = new DatastaxShardStateIO();
            metadataIO = new DatastaxMetadataIO();

        } else {

            shardStateIO = new AstyanaxShardStateIO();
            metadataIO = new AstyanaxMetadataIO();
        }
    }

    /**
     * @return a class for reading/writing ShardState
     */
    public ShardStateIO getShardStateIO() {
        return shardStateIO;
    }

    /**
     * @return a class for reading/writing Metadata
     */
    public MetadataIO getMetadataIO() {
        return metadataIO;
    }

    /**
     * An enumeration of the possible driver type
     */
    public static enum DriverType {
        ASTYANAX("astyanax"),
        DATASTAX("datastax");

        private DriverType(String driver) {
            name = driver;
        }

        private String name;

        public String getName() {
            return name;
        }

        /**
         * From a string value indicating driver type, return the corresponding
         * {@link com.rackspacecloud.blueflood.io.IOContainer.DriverType} value.
         * The typical use of this method is one where a driver type string is read
         * from a configuration file.
         *
         * @param driver
         * @return
         */
        public static DriverType getDriverType(String driver) {
            // if driver is explicitly datastax, then return DATASTAX
            // anything else, return ASTYANAX
            if ( ! Strings.isNullOrEmpty(driver) && DriverType.valueOf(driver.toUpperCase()) == DATASTAX ) {
                return DATASTAX;
            }
            return ASTYANAX;
        }
    }
}
