package org.apache.cassandra.db;

import java.util.Arrays;
import java.util.Iterator;

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

import static org.apache.cassandra.db.LegacyLayoutFrozenWhenNotFrozen.loadData;
import static org.apache.cassandra.db.LegacyLayoutFrozenWhenNotFrozen.queryById;

/*

Test data was generated using the following in 2.1

CREATE TABLE keyspace1.userlandcollection (
    id BIGINT PRIMARY KEY,
    options blob,
    members blob,
    events  blob
);
INSERT INTO Keyspace1.userlandcollection (id, options) VALUES (1, textAsBlob('{"42": "Answer to the Ultimate Question of Life, The Universe, and Everything"}'));
INSERT INTO Keyspace1.userlandcollection (id, members) VALUES (2, textAsBlob('{"first"}'));
INSERT INTO Keyspace1.userlandcollection (id, events)  VALUES (3, textAsBlob('["42"]'));
*/

/**
 * Tests when a SSTable is writen using a unknown encoding, but the schema is multi-cell.  This can happen via
 * Hadoop Bulk Writer, but also possible if data got copied from another cluster which had a different schema.
 */
public abstract class LegacyLayoutInvalidDataForMultiCellCollection
{
    static final String KEYSPACE = "keyspace1";

    private final ComplexCellWithoutCellPathBehavior fallThroughBehavior;

    protected LegacyLayoutInvalidDataForMultiCellCollection(ComplexCellWithoutCellPathBehavior behavior)
    {
        this.fallThroughBehavior = behavior;
        ComplexCellWithoutCellPathBehavior.set(ComplexCellWithoutCellPathBehavior.ATTEMPT_MIGRATE);
        ComplexCellWithoutCellPathBehavior.setFallThrough(behavior);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    /**
     * In this test there is a single SSTable, but the values do not match multi-cell or frozen collection type, either
     * each partition gets rejected or the values come back as null; no other response is allowed for this test
     */
    @Test
    public void testNonFrozenData() throws Throwable
    {
        QueryProcessor.executeInternal("CREATE TABLE keyspace1.notfrozen (\n" +
                                       "id BIGINT PRIMARY KEY,\n" +
                                       "options MAP<BIGINT,TEXT>,\n" +
                                       "members SET<TEXT>,\n" +
                                       "events  LIST<BIGINT>\n" +
                                       ");");

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("notfrozen");

        loadData("test/data/legacy-sstables/ka/legacy_tables/legacy_ka_userlandcollection", cfs);

        for (long expectedId : Arrays.asList(1L, 2L, 3L))
        {
            UntypedResultSet rs = queryById(fallThroughBehavior, cfs, expectedId);
            if (rs == null) // rejected
                continue;

            // behavior is not reject, so has to be log; aka all null collections
            Iterator<UntypedResultSet.Row> it = rs.iterator();
            Assert.assertTrue("No rows returned", it.hasNext());

            UntypedResultSet.Row row = it.next();
            Assert.assertEquals(expectedId, row.getLong("id"));
            Assert.assertEquals(null, row.getMap("options", LongType.instance, UTF8Type.instance));
            Assert.assertEquals(null, row.getSet("members", UTF8Type.instance));
            Assert.assertEquals(null, row.getList("events", LongType.instance));

            Assert.assertFalse("Only one row expected", it.hasNext());
        }
    }
}
