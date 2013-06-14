package com.cloudkick.blueflood.io;
import com.google.common.base.Function;
import com.netflix.astyanax.model.Row;

import java.util.concurrent.atomic.AtomicLong;

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
