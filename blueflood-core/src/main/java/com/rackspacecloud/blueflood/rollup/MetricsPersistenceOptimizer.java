/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.rollup;

/**
 * The intent of the interface is to provide standard API calls to users
 * that want to optimize metrics persistence. One typical optimization is
 * to query a database and check if the previous entry for the metric has
 * the same value as the current one. This is useful for string and state
 * metrics where we do not want to persist the same metric value multiple
 * times
 */
public interface MetricsPersistenceOptimizer {
    boolean shouldPersist(com.rackspacecloud.blueflood.types.Metric metric) throws Exception;
}