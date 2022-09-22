package com.rackspacecloud.blueflood.io.datastax;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.rackspacecloud.blueflood.test.CassandraUtilsIO;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Utility class to provide some test methods existing tests use.
 */
public class DCassandraUtilsIO implements CassandraUtilsIO {

    /**
     * Sets up the Blueflood keyspace in Cassandra. It relies on the "load.cdl" cql script that's referenced in many
     * places. To avoid needing cqlsh, this does some dumb, line-based parsing of the script and assembles the lines
     * into statements that it runs with the Cassandra driver. It should work fine, as long as nobody makes any crazy
     * changes to that script. We can use this to ensure a schema exists for integration tests, for example.
     */
    public void initDb() throws Exception {
        URL file = getClass().getResource("/cassandra/cli/load.cdl");
        List<String> lines = Files.readAllLines(Paths.get(file.toURI()));
        StringBuilder statementBuilder = new StringBuilder();
        try (DatastaxIO io = new DatastaxIO(DatastaxIO.Keyspace.NO_KEYSPACE, false)) {
            for (String line : lines) {
                if (line.startsWith("--")) {
                    // It's a comment
                    continue;
                }
                statementBuilder.append(' ');
                statementBuilder.append(line);
                if (line.trim().endsWith(";")) {
                    io.getInstanceSession().execute(statementBuilder.toString());
                    statementBuilder.setLength(0);
                }
            }
        }
    }

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
