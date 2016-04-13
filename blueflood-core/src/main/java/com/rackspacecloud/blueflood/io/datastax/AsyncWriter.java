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

import com.datastax.driver.core.ResultSetFuture;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;

/**
 * An interface describing the behavior of classes who can
 * asynchronously insert a metric to cassandra. This only
 * applies to classes that use datastax driver.
 */
public interface AsyncWriter {

    /**
     * Asynchronously insert a rolled up metric to the appropriate column family
     * for a particular granularity
     *
     * @param locator
     * @param column
     * @param rollup
     * @param granularity
     * @return
     */
    public ResultSetFuture putAsync(Locator locator, long column, Rollup rollup, Granularity granularity);
}
