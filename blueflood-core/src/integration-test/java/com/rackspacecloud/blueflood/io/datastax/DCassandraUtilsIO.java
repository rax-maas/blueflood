package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.CassandraUtilsIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxIO;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Utility class to provide some test methods existing tests use.
 */
public class DCassandraUtilsIO implements CassandraUtilsIO {

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

        Session session = DatastaxIO.getSession();

        Select select = select( "key" )
                .distinct()
                .from( cf );

        return session.execute( select ).all().size();
    }

    /**
     * Truncates the specified column family
     * @param cf
     * @throws Exception
     */
    @Override
    public void truncateColumnFamily( String cf ) throws Exception {

        Session session = DatastaxIO.getSession();

        int tries = 3;
        while (tries-- > 0) {
            try {
                session.execute(String.format("TRUNCATE %s", cf));
            } catch (Exception ex) {
                System.err.println(String.format("Error truncating %s. Remaining tries: %d. %s", cf, tries, ex.getMessage()));
                try { Thread.sleep(1000L); } catch (Exception ewww) {}
            }
        }

    }
}
