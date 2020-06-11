package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.LegacyLayout.ComplexCellWithoutCellPathBehavior;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;

/*

Test data was generated using the following in 2.1

CREATE TABLE keyspace1.frozencollections (
    id BIGINT PRIMARY KEY,
    options FROZEN<MAP<BIGINT,TEXT>>,
    members FROZEN<SET<TEXT>>,
    events  FROZEN<LIST<BIGINT>>
);
INSERT INTO Keyspace1.frozencollections (id, options, members, events) VALUES (1, {42: 'Answer to the Ultimate Question of Life, The Universe, and Everything'}, {'first'}, [42]);
INSERT INTO Keyspace1.frozencollections (id, options, members, events) VALUES (2, {42: 'Answer to the Ultimate Question of Life, The Universe, and Everything'}, {'first'}, [42]);
-- nodetool flush
-- this will conflict and overwrite the values for partition 1 in the first sstable; no conflict is expected for partition 2
INSERT INTO Keyspace1.frozencollections (id, options, members, events) VALUES (1, {10: 'There are only 10 types of people in the world: Those who understand binary, and those who don’t'}, {'second'}, [10]);
-- expected output via 2.1
cqlsh> select * from Keyspace1.frozencollections;

 id | events | members    | options
----+--------+------------+----------------------------------------------------------------------------------------------------------
  2 |   [42] |  {'first'} |                            {42: 'Answer to the Ultimate Question of Life, The Universe, and Everything'}
  1 |   [10] | {'second'} | {10: 'There are only 10 types of people in the world: Those who understand binary, and those who don’t'}
*/

/**
 * Tests when a SSTable is writen using a frozen collection, but the schema is multi-cell.  This can happen via
 * Hadoop Bulk Writer, but also possible if data got copied from another cluster which had a different schema.
 */
public abstract class LegacyLayoutFrozenWhenNotFrozen
{
    static final String KEYSPACE = "keyspace1";

    private final ComplexCellWithoutCellPathBehavior behavior;

    protected LegacyLayoutFrozenWhenNotFrozen(ComplexCellWithoutCellPathBehavior behavior)
    {
        this.behavior = behavior;
        ComplexCellWithoutCellPathBehavior.set(behavior);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    /**
     * Test accesses row 2 which only exists in sstable-1;his validates the different behaviors in isolation
     */
    @Test
    public void testFrozenCollectionsNoShadowing() throws Throwable
    {
        QueryProcessor.executeInternal("CREATE TABLE keyspace1.noshadowing (\n" +
                                       "id BIGINT PRIMARY KEY,\n" +
                                       "options MAP<BIGINT,TEXT>,\n" +
                                       "members SET<TEXT>,\n" +
                                       "events  LIST<BIGINT>\n" +
                                       ");");

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("noshadowing");

        loadData("test/data/legacy-sstables/ka/legacy_tables/legacy_ka_frozencollections", cfs);

        long expectedId = 2L;
        UntypedResultSet rs = queryById(cfs, expectedId);
        if (rs == null) // rejected
            return;
        // behavior is not reject, its either log or migrate
        Iterator<UntypedResultSet.Row> it = rs.iterator();
        Assert.assertTrue("No rows returned", it.hasNext());

        behaviorAwareAssertEquals(it.next(), expectedId,
                                  ImmutableMap.of(42L, "Answer to the Ultimate Question of Life, The Universe, and Everything"),
                                  ImmutableSet.of("first"),
                                  ImmutableList.of(42L));

        Assert.assertFalse("Only one row expected", it.hasNext());
    }

    /**
     * In this test there are 2 SSTables with different version of the frozen types, this test should validate the
     * expected behavior in each of the 3 supported cases (null, error, migrate).
     */
    @Test
    public void testFrozenCollectionsShadowing() throws Throwable
    {
        QueryProcessor.executeInternal("CREATE TABLE keyspace1.shadowing (\n" +
                                       "id BIGINT PRIMARY KEY,\n" +
                                       "options MAP<BIGINT,TEXT>,\n" +
                                       "members SET<TEXT>,\n" +
                                       "events  LIST<BIGINT>\n" +
                                       ");");

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("shadowing");

        loadData("test/data/legacy-sstables/ka/legacy_tables/legacy_ka_frozencollections", cfs);

        long expectedId = 1L;
        UntypedResultSet rs = queryById(cfs, expectedId);
        if (rs == null) // rejected
            return;

        // behavior is not reject, its either log or migrate
        Iterator<UntypedResultSet.Row> it = rs.iterator();
        Assert.assertTrue("No rows returned", it.hasNext());

        behaviorAwareAssertEquals(it.next(), expectedId,
                                  ImmutableMap.of(10L, "There are only 10 types of people in the world: Those who understand binary, and those who don’t"),
                                  ImmutableSet.of("second"),
                                  ImmutableList.of(10L));

        Assert.assertFalse("Only one row expected", it.hasNext());
    }

    private void behaviorAwareAssertEquals(UntypedResultSet.Row row,
                                           long expectedId,
                                           Map<Long, String> expectedOptions,
                                           Set<String> expectedMemebers,
                                           List<Long> expectedEvents)
    {
        long id = row.getLong("id");
        Map<Long, String> options = row.getMap("options", LongType.instance, UTF8Type.instance);
        Set<String> members = row.getSet("members", UTF8Type.instance);
        List<Long> events = row.getList("events", LongType.instance);

        Assert.assertEquals(expectedId, id);
        if (behavior == ComplexCellWithoutCellPathBehavior.ATTEMPT_MIGRATE)
        {
            Assert.assertEquals(expectedOptions, options);
            Assert.assertEquals(expectedMemebers, members);
            Assert.assertEquals(expectedEvents, events);
        }
        else if (behavior == ComplexCellWithoutCellPathBehavior.LOG)
        {
            Assert.assertEquals(null, options);
            Assert.assertEquals(null, members);
            Assert.assertEquals(null, events);
        }
        else
        {
            throw new UnsupportedOperationException("Unexpected behavior: " + behavior.name());
        }
    }

    private UntypedResultSet queryById(ColumnFamilyStore cfs, long expectedId)
    {
        return queryById(behavior, cfs, expectedId);
    }

    static UntypedResultSet queryById(ComplexCellWithoutCellPathBehavior behavior, ColumnFamilyStore cfs, long expectedId)
    {
        try
        {
            UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE id=?", cfs.keyspace.getName(), cfs.name), expectedId);
            if (behavior == ComplexCellWithoutCellPathBehavior.REJECT)
                throw new IllegalStateException("Expected query to be rejected but did not");
            return rs;
        }
        catch (AssertionError e)
        {
            if (behavior != ComplexCellWithoutCellPathBehavior.REJECT)
                throw e;
            // behavior is to reject, so nothing to process
            return null;
        }
    }

    static void loadData(String path, ColumnFamilyStore cfs) throws IOException
    {
        File legacySSTableRoot = new File(path);
        File[] children = legacySSTableRoot.listFiles();
        if (children == null || children.length == 0)
            throw new AssertionError("Unable to find sstables in path " + path);

        String ks = cfs.keyspace.getName();
        String table = cfs.name;
        Path outputPath = cfs.getDirectories().getDirectoryForNewSSTables().toPath();
        for (File f : children)
        {
            String renamed = renameToTable(ks, table, f.getName());
            Files.copy(f.toPath(), outputPath.resolve(renamed));
        }

        cfs.loadNewSSTables();
    }

    private static String renameToTable(String ks, String table, String name)
    {
        String[] split = name.split("-");
        // sample: ks-table-ka-1-TOC.txt
        assert split.length == 5 : "unable to parse sstable name, expected 5 elements but given " + Arrays.toString(split);
        StringBuilder sb = new StringBuilder();
        sb.append(ks).append("-").append(table).append("-");
        sb.append(split[2]).append("-").append(split[3]).append("-").append(split[4]);
        return sb.toString();
    }
}
