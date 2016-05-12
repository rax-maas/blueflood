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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxExcessEnumIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxLocatorIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxMetadataIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxExcessEnumIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxLocatorIO;
import com.rackspacecloud.blueflood.io.datastax.DMetadataIO;
import com.rackspacecloud.blueflood.io.datastax.DShardStateIO;
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
    private static IOContainer FROM_CONFIG_INSTANCE = null;

    private ShardStateIO shardStateIO;
    private MetadataIO metadataIO;
    private LocatorIO locatorIO;
    private ExcessEnumIO excessEnumIO;

    // more IO classes to follow

    /**
     * Returns an instance of this class based on what configuration says our
     * driver should be.
     *
     * @return IOContainer
     */
    public static synchronized IOContainer fromConfig() {
        if ( FROM_CONFIG_INSTANCE == null ) {
                String driver = configuration.getStringProperty(CoreConfig.CASSANDRA_DRIVER);
                FROM_CONFIG_INSTANCE = new IOContainer(DriverType.getDriverType(driver));
        }
        return FROM_CONFIG_INSTANCE;
    }

    @VisibleForTesting
    static synchronized void resetInstance() {
        FROM_CONFIG_INSTANCE = null;
    }

    /**
     * Construct an instance of this class based on the specified
     * {@link com.rackspacecloud.blueflood.io.IOContainer.DriverType}
     *
     * @param driver
     */
    public IOContainer(DriverType driver) {

        if ( driver == DriverType.DATASTAX ) {

            metadataIO = new DMetadataIO();
            shardStateIO = new DShardStateIO();
            locatorIO = new DatastaxLocatorIO();
            excessEnumIO = new DatastaxExcessEnumIO();

        } else {

            metadataIO = new AstyanaxMetadataIO();
            shardStateIO = new AstyanaxShardStateIO();
            locatorIO = new AstyanaxLocatorIO();
            excessEnumIO = new AstyanaxExcessEnumIO();
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
     * @return a class for reading/writing Locators
     */
    public LocatorIO getLocatorIO() {
        return locatorIO;
    }

    /**
     * @return a class for reading/writing Excess Enum Metrics
     */
    public ExcessEnumIO getExcessEnumIO() {
        return excessEnumIO;
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
