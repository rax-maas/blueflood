package com.rackspacecloud.blueflood.io.astyanax;

import com.google.common.base.Function;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.io.CassandraUtilsIO;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class to provide some test methods existing tests use.
 */
public class ACassandraUtilsIO implements CassandraUtilsIO {

    /**
     * Count the number of keys in the table.
     *
     * Originally this was called getRowCount, and Astyanax would return the number of "keys".
     * Later, Datastax, doing what looked like the same query, returned the number of [key,value...]
     * tuples, and keys would be counted multiple times, if they had multiple values.
     *
     * Preserving Astyanax results and calling it getKeyCount
     *
     * @param cf
     *
     * @return
     *
     * @throws Exception
     */
    @Override
    public long getKeyCount( String cf ) throws Exception {

        ColumnFamily<String, String> columnFamily = ColumnFamily.newColumnFamily(cf, StringSerializer.get(), StringSerializer.get());
        AstyanaxRowCounterFunction<String, String> rowCounter = new AstyanaxRowCounterFunction<String, String>();
        boolean result = new AllRowsReader.Builder<String, String>( AstyanaxIO.getKeyspace(), columnFamily)
                .withColumnRange(null, null, false, 0)
                .forEachRow(rowCounter)
                .build()
                .call();

        return rowCounter.getCount();
    }

    @Override
    public void truncateColumnFamily( String cf ) throws Exception {
        int tries = 3;
        while (tries-- > 0) {
            try {
                AstyanaxIO.getKeyspace().truncateColumnFamily(cf);
            } catch (ConnectionException ex) {
                System.err.println(String.format("Error truncating %s. Remaining tries: %d. %s", cf, tries, ex.getMessage()));
                try { Thread.sleep(1000L); } catch (Exception ewww) {}
            }
        }
    }

    /**
     * Simple function to count the number of rows/keys.
     *
     * @author elandau
     *
     * @param <K>
     * @param <C>
     */
    // Copy-pasted from astyanax 1.56.37, it doesn't exist in 1.56.32 which we use.
    class AstyanaxRowCounterFunction<K,C> implements Function<Row<K,C>, Boolean> {

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
}
