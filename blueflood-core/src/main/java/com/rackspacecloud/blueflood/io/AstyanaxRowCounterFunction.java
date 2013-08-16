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

package com.rackspacecloud.blueflood.io;
import com.google.common.base.Function;
import com.netflix.astyanax.model.Row;

import java.util.concurrent.atomic.AtomicLong;

// todo: This belongs in the test code. IntegrationTestBase depends on it though. So we have to wait until
// IntegrationTestBase is moved back into the test section of the repository.
/**
 * Simple function to counter the number of rows
 *
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
// Copy-pasted from astyanax 1.56.37, it doesn't exist in 1.56.32 which we use.
public class AstyanaxRowCounterFunction<K,C> implements Function<Row<K,C>, Boolean> {

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public Boolean apply(Row<K,C> input) {
        counter.incrementAndGet();
        return true;
    }

    public long getCount() {
        return counter.get();
    }

    public void reset() {
        counter.set(0);
    }
}
