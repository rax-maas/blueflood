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

import com.rackspacecloud.blueflood.types.Locator;

import java.io.IOException;
import java.util.Set;

public interface ExcessEnumIO {

    /**
     * Gets all the ExcessEnum Metrics Locators
     * @return Set of Locators of enum values that has exceeded the number of unique string values CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD
     * @throws IOException
     */
    public Set<Locator> getExcessEnumMetrics() throws IOException;

    /**
     * Insert locator value for an enum metric that has exceeded the number of unique string values CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD
     * @param locator
     * @throws IOException
     */
    public void insertExcessEnumMetric(Locator locator) throws IOException;

}
