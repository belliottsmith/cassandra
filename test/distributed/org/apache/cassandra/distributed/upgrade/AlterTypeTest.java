package org.apache.cassandra.distributed.upgrade;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.QueryResult;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.utils.UUIDGen;

public class AlterTypeTest extends UpgradeTestBase
{
    @Test
    public void alterTypeWithConflictingKeys() throws Throwable
    {
        new TestCase()
        .nodes(1)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            IUpgradeableInstance node = cluster.get(1);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".mismatch (key text primary key, value timeuuid)");

            // write valid timeuuid then flush so its on disk
            node.executeInternal("INSERT INTO " + KEYSPACE + ".mismatch (key, value) VALUES (?, ?)", "k1", UUIDGen.getTimeUUID());
            node.flush(KEYSPACE);

            // alter schema, changing from timeuuid to uuid
            Object[] columns = node.executeInternal("SELECT keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type, type, validator " +
                                                    "FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=? AND column_name=?", KEYSPACE, "mismatch", "value")[0];
            columns[columns.length - 1] = "org.apache.cassandra.db.marshal.UUIDType";
            node.executeInternal("INSERT INTO system.schema_columns (keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type, type, validator) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", columns);

            // trigger schema to be reloaded
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".mismatch WITH comment='upgrade'");

            // add a random UUID (if the above steps were not successful then this should fail with: Invalid version for TimeUUID type)
            node.executeInternal("INSERT INTO " + KEYSPACE + ".mismatch (key, value) VALUES (?, ?)", "k1", UUID.randomUUID());
            node.flush(KEYSPACE);
        })
        .runAfterClusterUpgrade(cluster -> {
            ICoordinator coordinator = cluster.coordinator(1);
            IUpgradeableInstance node = cluster.get(1);
            // SerializableRunnable isn't needed, just a runnable which allows exceptions
            for (SerializableRunnable setup : Arrays.<SerializableRunnable>asList(
            // this step does nothing, makes sure the query is success with 2.x sstables; mostly here to make the code simpler
            () -> {},
            // upgrade the sstables to 3.x to make sure the query is fine
            () -> node.nodetoolResult("upgradesstables", "-a").asserts().success(),
            // merge the sstables together
            () -> node.forceCompact(KEYSPACE, "mismatch")
            ))
            {
                setup.run();

                QueryResult results = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".mismatch WHERE key=?", ConsistencyLevel.ONE, "k1");
                Assert.assertTrue("No rows found", results.hasNext());
                Assert.assertEquals("UUID should be random but was not", 4, results.next().getUUID("value").version());
            }
        })
        .run();
    }
}
