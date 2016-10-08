package org.apache.cassandra.service;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.apache.cassandra.cql3.QueryProcessor.process;

/**
 *
 */
public class PartitionBlacklistTest extends SchemaLoader
{
    @BeforeClass
    public static void init() throws ConfigurationException, InterruptedException, RequestExecutionException, UnsupportedEncodingException
    {
        SchemaLoader.schemaDefinition("");
        DatabaseDescriptor.setEnablePartitionBlacklist(true);
        DatabaseDescriptor.setEnableBlacklistRangeReads(true);
        DatabaseDescriptor.setBlacklistConsistencyLevel(ConsistencyLevel.ONE);
        DatabaseDescriptor.setBlacklistRefreshPeriodSeconds(1);
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup() throws RequestExecutionException, UnsupportedEncodingException, InterruptedException {
        DatabaseDescriptor.setEnablePartitionBlacklist(true);
        process("TRUNCATE cie_internal.partition_blacklist", ConsistencyLevel.ONE);
        forceReloadBlacklist();

        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('aaa', 'bbb', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('ccc', 'ddd', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('ddd', 'eee', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);

        blacklist("cql_keyspace", "foofoo", "bbb:ccc");
        forceReloadBlacklist();
    }


    private static void blacklist(final String ks, final String cf, final String key) throws RequestExecutionException
    {
        StorageProxy.instance.blacklistKey(ks, cf, key);
    }

    @Test
    public void testRead() throws RequestExecutionException
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE bar='aaa' and baz='bbb'", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testReadBlacklisted() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE bar='bbb' and baz='ccc'", ConsistencyLevel.ONE);
    }

    @Test
    public void testReadUnblacklisted() throws RequestExecutionException, InterruptedException, UnsupportedEncodingException
    {
        process("TRUNCATE cie_internal.partition_blacklist", ConsistencyLevel.ONE);
        forceReloadBlacklist();
        process("SELECT * FROM cql_keyspace.foofoo WHERE bar='bbb' and baz='ccc'", ConsistencyLevel.ONE);
    }

    @Test
    public void testWrite() throws RequestExecutionException
    {
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('eee', 'fff', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("DELETE FROM cql_keyspace.foofoo WHERE bar='eee' and baz='fff'", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testWriteBlacklisted() throws Throwable
    {
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testCASWriteBlacklisted() throws Throwable
    {
        process("UPDATE cql_keyspace.foofoo SET foo='w' WHERE bar='bbb' AND baz='ccc' AND qux='eee' AND quz='fff' IF foo='v'", ConsistencyLevel.LOCAL_SERIAL);
    }

    @Test
    public void testWriteUnblacklisted() throws RequestExecutionException, InterruptedException, UnsupportedEncodingException
    {
        process("TRUNCATE cie_internal.partition_blacklist", ConsistencyLevel.ONE);
        forceReloadBlacklist();
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
    }

    @Test
    public void testRangeSlice() throws RequestExecutionException
    {
        UntypedResultSet rows;
        rows = process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) < token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(1, rows.size());

        rows = process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) > token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(2, rows.size());

        rows = process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) >= token('aaa', 'bbb') and token(bar, baz) < token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(1, rows.size());

        rows = process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) > token('bbb', 'ccc') and token(bar, baz) <= token('ddd', 'eee')", ConsistencyLevel.ONE);
        Assert.assertEquals(2, rows.size());
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeBlacklisted1() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeBlacklisted2() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) >= token('aaa', 'bbb') and token (bar, baz) <= token('bbb', 'ccc')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeBlacklisted3() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) >= token('bbb', 'ccc') and token (bar, baz) <= token('ccc', 'ddd')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeBlacklisted4() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) > token('aaa', 'bbb') and token (bar, baz) < token('ccc', 'ddd')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeBlacklisted5() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) > token('aaa', 'bbb')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeBlacklisted6() throws Throwable
    {
        process("SELECT * FROM cql_keyspace.foofoo WHERE token(bar, baz) < token('ddd', 'eee')", ConsistencyLevel.ONE);
    }

    @Test
    public void testReadInvalidCF() throws Exception
    {
        blacklist("santa", "claus", "hohoho");
        forceReloadBlacklist();
    }

    @Test
    public void testBlacklistDisabled() throws Exception
    {
        DatabaseDescriptor.setEnablePartitionBlacklist(false);
        process("INSERT INTO cql_keyspace.foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
        process("SELECT * FROM cql_keyspace.foofoo WHERE bar='bbb' and baz='ccc'", ConsistencyLevel.ONE);
        process("SELECT * FROM cql_keyspace.foofoo", ConsistencyLevel.ONE);
    }

    private static void forceReloadBlacklist() throws InterruptedException
    {
        StorageProxy.instance.loadPartitionBlacklist();
    }
}
