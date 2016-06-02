package com.rackspacecloud.blueflood.io;

/**
 * Utility class to provide some test methods existing tests use.
 */
public interface CassandraUtilsIO {

    /**
     * Count the number of keys in the table.
     *
     * Originally this was called getRowCount, and Astyanax would return the number of "keys".
     * Later, Datastax, doing what looked like the same query, returned the number of [key,value...]
     * tuples, and keys would be counted multiple times, if they had multiple values.
     *
     * Preserving Astyanax results and calling it getKeyCount
     *
     * @param cf table name
     *
     * @return number of keys
     *
     * @throws Exception
     */
    public long getKeyCount( String cf ) throws Exception;

    /**
     * Truncates the specified column family
     * @param cf
     * @throws Exception
     */
    public void truncateColumnFamily ( String cf ) throws Exception;
}
