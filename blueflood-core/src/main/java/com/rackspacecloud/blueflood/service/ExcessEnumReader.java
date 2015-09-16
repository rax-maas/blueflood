
/*
 * Copyright 2015 Rackspace
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


package com.rackspacecloud.blueflood.service;

import java.util.Set;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.types.Locator;

public class ExcessEnumReader implements Runnable{

    private static final ExcessEnumReader INSTANCE = new ExcessEnumReader();
    public static ExcessEnumReader getInstance() {
        return INSTANCE;
    }

    private Set<Locator> excessEnumMetrics;
    private static final Configuration config = Configuration.getInstance();

    public Boolean isInExcessEnumMetrics(Locator m){
        return excessEnumMetrics.contains(m);
    }
    final public void run() {

        int sleepMillis = config.getIntegerProperty(CoreConfig.EXCESS_ENUM_READER_SLEEP);

        while (true) try {
            excessEnumMetrics = AstyanaxReader.getInstance().getExcessEnumMetrics();
            Thread.sleep(sleepMillis);
        } catch (Exception e) {
            throw new RuntimeException("ExcessEnumReader failed with exception", e);
        }
    }
}
