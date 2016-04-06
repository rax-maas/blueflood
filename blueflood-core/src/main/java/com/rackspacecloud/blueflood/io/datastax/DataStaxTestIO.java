package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.rackspacecloud.blueflood.io.TestIO;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Utility class to provide some test methods existing tests use.
 */
public class DatastaxTestIO implements TestIO {

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
                .from( cf );

        List<Row> rows = session.execute( select ).all();
        Set<String> keys = new HashSet<String>();

        for( Row r : rows ) {

            keys.add( r.getString( "key" ) );
        }

        return keys.size();
    }
}
