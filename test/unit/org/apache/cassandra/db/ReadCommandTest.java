/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadCommandTest
{
    private static final String KEYSPACE = "ReadCommandTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";
    private static final String CF3 = "Counter1";

    private static final InetAddress REPAIR_COORDINATOR;
    static {
        try
        {
            REPAIR_COORDINATOR = InetAddress.getByName("10.0.0.1");
        }
        catch (UnknownHostException e)
        {

            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.setDaemonInitialized();

        CFMetaData metadata1 =
        CFMetaData.Builder.create(KEYSPACE, CF1)
                          .addPartitionKey("key", BytesType.instance)
                          .addClusteringColumn("col", AsciiType.instance)
                          .addRegularColumn("a", AsciiType.instance)
                          .addRegularColumn("b", AsciiType.instance)
                          .build();

        CFMetaData metadata2 =
        CFMetaData.Builder.create(KEYSPACE, CF2)
                          .addPartitionKey("key", BytesType.instance)
                          .addClusteringColumn("col", AsciiType.instance)
                          .addRegularColumn("a", AsciiType.instance)
                          .addRegularColumn("b", AsciiType.instance)
                          .build()
                          .caching(CachingParams.CACHE_EVERYTHING);
        CFMetaData metadata3 =
        CFMetaData.Builder.create(KEYSPACE, CF3, false, true, false, true)
                          .addPartitionKey("key", BytesType.instance)
                          .addClusteringColumn("col", AsciiType.instance)
                          .addRegularColumn("c", CounterColumnType.instance)
                          .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata1, metadata2, metadata3);

        ActiveRepairService.instance.start();
    }


    @Test
    public void testSinglePartitionSliceRepairedDataTracking() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        testRepairedDataTracking(cfs, readCommand);
    }

    @Test
    public void testPartitionRangeRepairedDataTracking() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        ReadCommand readCommand = Util.cmd(cfs).build();
        testRepairedDataTracking(cfs, readCommand);
    }

    @Test
    public void testSinglePartitionNamesRepairedDataTracking() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("cc").includeRow("dd").build();
        testRepairedDataTracking(cfs, readCommand);
    }

    @Test
    public void testSinglePartitionNamesSkipsOptimisationsIfTrackingRepairedData()
    {
        // when tracking, the optimizations of querying sstables in timestamp order and
        // returning once all requested columns are not available so just assert that
        // all sstables are read when performing such queries
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.metadata.params(TableParams.builder(cfs.metadata.params)
                                       .caching(CachingParams.CACHE_NOTHING)
                                       .build());

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
            .clustering("dd")
            .add("a", ByteBufferUtil.bytes("abcd"))
            .build()
            .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 1, ByteBufferUtil.bytes("key"))
            .clustering("dd")
            .add("a", ByteBufferUtil.bytes("wxyz"))
            .build()
            .apply();

        cfs.forceBlockingFlush();
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        Collections.sort(sstables, SSTableReader.maxTimestampComparator);

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).includeRow("dd").columns("a").build();

        assertEquals(0, readCount(sstables.get(0)));
        assertEquals(0, readCount(sstables.get(1)));
        ReadCommand withTracking = readCommand.copy();
        withTracking.trackRepairedStatus();
        Util.getAll(withTracking);
        assertEquals(1, readCount(sstables.get(0)));
        assertEquals(1, readCount(sstables.get(1)));

        // same command without tracking touches only the table with the higher timestamp
        Util.getAll(readCommand.copy());
        assertEquals(2, readCount(sstables.get(0)));
        assertEquals(1, readCount(sstables.get(1)));
    }

    @Test
    public void dontIncludeLegacyCounterContextInDigest() throws IOException
    {
        // Serializations of a CounterContext containing legacy (pre-2.1) shards
        // can legitmately differ across replicas. For this reason, the context
        // bytes are omitted from the repaired digest if they contain legacy shards.
        // This clearly has a tradeoff with the efficacy of the digest, without doing
        // so false positive digest mismatches will be reported for scenarios where
        // there is nothing that can be done to "fix" the replicas
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF3);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        // insert a row with the counter column having value 0, in a legacy shard.
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("aa")
                .addLegacyCounterCell("c", 0L)
                .build()
                .apply();
        cfs.forceBlockingFlush();
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        // execute a read and capture the digest
        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        ByteBuffer digestWithLegacyCounter0 = performReadAndVerifyRepairedInfo(readCommand, 1, 1, true);
        assertFalse(ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(digestWithLegacyCounter0));

        // truncate, then re-insert the same partition, but this time with a legacy
        // shard having the value 1. The repaired digest should match the previous, as
        // the values (context) are not included, only the cell metadata (ttl, timestamp, etc)
        cfs.truncateBlocking();
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("aa")
                .addLegacyCounterCell("c", 1L)
                .build()
                .apply();
        cfs.forceBlockingFlush();
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        ByteBuffer digestWithLegacyCounter1 = performReadAndVerifyRepairedInfo(readCommand, 1, 1, true);
        assertEquals(digestWithLegacyCounter0, digestWithLegacyCounter1);

        // truncate, then re-insert the same partition, but this time with a non-legacy
        // counter cell present. The repaired digest should not match the previous ones
        // as this time the value (context) is included.
        cfs.truncateBlocking();
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("aa")
                .add("c", 1L)
                .build()
                .apply();
        cfs.forceBlockingFlush();
        cfs.getLiveSSTables().forEach(sstable -> mutateRepaired(cfs, sstable, 111, null));

        ByteBuffer digestWithCounterCell = performReadAndVerifyRepairedInfo(readCommand, 1, 1, true);
        assertFalse(ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(digestWithCounterCell));
        assertFalse(digestWithLegacyCounter0.equals(digestWithCounterCell));
        assertFalse(digestWithLegacyCounter1.equals(digestWithCounterCell));
    }

    private long readCount(SSTableReader sstable)
    {
        return sstable.getReadMeter().count();
    }

    @Test
    public void skipRowCacheIfTrackingRepairedData()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2);

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.metadata.params(TableParams.builder(cfs.metadata.params)
                                       .caching(CachingParams.CACHE_EVERYTHING)
                                       .build());

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        assertTrue(cfs.isRowCacheEnabled());
        // warm the cache
        assertFalse(Util.getAll(readCommand).isEmpty());
        long cacheHits = cfs.metric.rowCacheHit.getCount();

        Util.getAll(readCommand);
        assertTrue(cfs.metric.rowCacheHit.getCount() > cacheHits);
        cacheHits = cfs.metric.rowCacheHit.getCount();

        ReadCommand withRepairedInfo = readCommand.copy();
        withRepairedInfo.trackRepairedStatus();
        Util.getAll(withRepairedInfo);
        assertEquals(cacheHits, cfs.metric.rowCacheHit.getCount());
    }

    private void testRepairedDataTracking(ColumnFamilyStore cfs, ReadCommand readCommand) throws IOException
    {
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key"))
                .clustering("cc")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 1, ByteBufferUtil.bytes("key"))
                .clustering("dd")
                .add("a", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        cfs.forceBlockingFlush();
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        sstables.forEach(sstable -> assertFalse(sstable.isRepaired() || sstable.isPendingRepair()));
        SSTableReader sstable1 = sstables.get(0);
        SSTableReader sstable2 = sstables.get(1);

        int numPartitions = 1;
        int rowsPerPartition = 2;

        // Capture all the digest versions as we mutate the table's repaired status. Each time
        // we make a change, we expect a different digest.
        Set<ByteBuffer> digests = new HashSet<>();
        // first time round, nothing has been marked repaired so we expect digest to be an empty buffer and to be marked conclusive
        ByteBuffer digest = performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, true);
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, digest);
        digests.add(digest);

        // add a pending repair session to table1, digest should remain the same but now we expect it to be marked inconclusive
        UUID session1 = UUIDGen.getTimeUUID();
        mutateRepaired(cfs, sstable1, ActiveRepairService.UNREPAIRED_SSTABLE, session1);
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(1, digests.size());

        // add a different pending session to table2, digest should remain the same and still consider it inconclusive
        UUID session2 = UUIDGen.getTimeUUID();
        mutateRepaired(cfs, sstable2, ActiveRepairService.UNREPAIRED_SSTABLE, session2);
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(1, digests.size());

        // mark one table repaired
        mutateRepaired(cfs, sstable1, 111, null);
        // this time, digest should not be empty, session2 still means that the result is inconclusive
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, false));
        assertEquals(2, digests.size());

        // mark the second table repaired
        mutateRepaired(cfs, sstable2, 222, null);
        // digest should be updated again and as there are no longer any pending sessions, it should be considered conclusive
        digests.add(performReadAndVerifyRepairedInfo(readCommand, numPartitions, rowsPerPartition, true));
        assertEquals(3, digests.size());

        // insert a partition tombstone into the memtable, then re-check the repaired info.
        // This is to ensure that when the optimisations which skip reading from sstables
        // when a newer partition tombstone has already been cause the digest to be marked
        // as inconclusive.
        // the exception to this case is for partition range reads, where we always read
        // and generate digests for all sstables, so we only test this path for single partition reads
        if (readCommand.isLimitedToOnePartition())
        {
            new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata,
                                                             ByteBufferUtil.bytes("key"),
                                                             FBUtilities.timestampMicros(),
                                                             FBUtilities.nowInSeconds())).apply();
            digest = performReadAndVerifyRepairedInfo(readCommand, 0, rowsPerPartition, false);
            assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, digest);

            // now flush so we have an unrepaired table with the deletion and repeat the check
            cfs.forceBlockingFlush();
            digest = performReadAndVerifyRepairedInfo(readCommand, 0, rowsPerPartition, false);
            assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, digest);
        }
    }

    private void mutateRepaired(ColumnFamilyStore cfs, SSTableReader sstable, long repairedAt, UUID pendingSession)
    {
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingSession);
            sstable.reloadSSTableMetadata();
            if (pendingSession != null)
            {
                // setup a minimal repair session. This is necessary because we
                // check for sessions which have exceeded timeout and been purged
                Range<Token> range = new Range<>(cfs.metadata.partitioner.getMinimumToken(),
                                                 cfs.metadata.partitioner.getRandomToken());
                ActiveRepairService.instance.registerParentRepairSession(pendingSession,
                                                                         REPAIR_COORDINATOR,
                                                                         Lists.newArrayList(cfs),
                                                                         Sets.newHashSet(range),
                                                                         true,
                                                                         repairedAt,
                                                                         true,
                                                                         PreviewKind.NONE);

                LocalSessionAccessor.prepareUnsafe(pendingSession, null, Sets.newHashSet(REPAIR_COORDINATOR));
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer performReadAndVerifyRepairedInfo(ReadCommand command,
                                                        int expectedPartitions,
                                                        int expectedRowsPerPartition,
                                                        boolean expectConclusive)
    {
        // perform equivalent read command multiple times and assert that
        // the repaired data info is always consistent. Return the digest
        // so we can verify that it changes when the repaired status of
        // the queried tables does.
        Set<ByteBuffer> digests = new HashSet<>();
        for (int i = 0; i < 10; i++)
        {
            ReadCommand withRepairedInfo = command.copy();
            withRepairedInfo.trackRepairedStatus();

            List<FilteredPartition> partitions = Util.getAll(withRepairedInfo);
            assertEquals(expectedPartitions, partitions.size());
            partitions.forEach(p -> assertEquals(expectedRowsPerPartition, p.rowCount()));

            ByteBuffer digest = withRepairedInfo.getRepairedDataDigest();
            digests.add(digest);
            assertEquals(1, digests.size());
            assertEquals(expectConclusive, withRepairedInfo.isRepairedDataDigestConclusive());
        }
        return digests.iterator().next();
    }

}
