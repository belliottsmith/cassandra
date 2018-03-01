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

package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PreviewDumpTest
{
    private static final String ks = "ks";
    private static final String tbl = "tbl";
    private static final CFMetaData cfm = CFMetaData.compile(String.format("CREATE TABLE %s.%s (k int primary key, v int)", ks, tbl), ks);
    private static ColumnFamilyStore cfs = null;

    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.setDebugValidationPreviewEnabled(true);
        Assert.assertEquals(ByteOrderedPartitioner.instance, partitioner);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.cfId);

        for (int i=0; i<100; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (%s, %s)", ks, tbl, i, i));
        }
        cfs.forceBlockingFlush();
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, FBUtilities.nowInSeconds(), null);
        sstable.reloadSSTableMetadata();

        // Turn off out of range token rejection so we don't need to set up token metadata
        StorageService.instance.setOutOfTokenRangeRequestRejectionEnabled(false);
    }

    private static Token tk(int key)
    {
        return partitioner.getToken(ByteBufferUtil.bytes(key));
    }


    /**
     * We only need to verify all the expected keys come out of the serialized stream files
     */
    private static abstract class NoopMultiWriter implements SSTableMultiWriter
    {

        public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
        {
            return null;
        }

        public Collection<SSTableReader> finish(boolean openResult)
        {
            return null;
        }

        public Collection<SSTableReader> finished()
        {
            return null;
        }

        public SSTableMultiWriter setOpenResult(boolean openResult)
        {
            return null;
        }

        public String getFilename()
        {
            return null;
        }

        public long getFilePointer()
        {
            return 0;
        }

        public UUID getCfId()
        {
            return null;
        }

        public Throwable commit(Throwable accumulate)
        {
            return null;
        }

        public Throwable abort(Throwable accumulate)
        {
            return null;
        }

        public void prepareToCommit()
        {

        }

        public void close()
        {

        }

        public DecoratedKey getMinKey()
        {
            return null;
        }

        public DecoratedKey getMaxKey()
        {
            return null;
        }
    }

    @Test
    public void previewTest() throws Exception
    {
        Range<Token> range1 = new Range<>(tk(10), tk(20));
        Range<Token> range2 = new Range<>(tk(50), tk(60));
        Collection<Range<Token>> ranges = ImmutableList.of(range1, range2);

        StreamSession session = new StreamSession(InetAddress.getByName("127.0.0.1"),
                                                  InetAddress.getByName("127.0.0.2"),
                                                  null,
                                                  0,
                                                  true,
                                                  null,
                                                  PreviewKind.REPAIRED);
        UUID planId = UUIDGen.getTimeUUID();
        session.init(new StreamResultFuture(planId, "Repair", true, null, PreviewKind.REPAIRED));
        session.prepare(Collections.singleton(new StreamRequest(ks, ranges, Collections.singleton(tbl))), Collections.emptyList());

        File expectedDir = new File(Directories.dataDirectories[0].location, String.format("../previews/%s/%s/%s", planId, ks, tbl));
        Assert.assertTrue(expectedDir.exists());
        Assert.assertEquals(1, FileUtils.listFiles(expectedDir, null, true).size());
        File expectedFile = new File(expectedDir, "0.bin");
        Assert.assertTrue(expectedFile.exists());

        Set<Integer> expectedKeys = new HashSet<>();
        for (int i=0; i<10; i++)
        {
            expectedKeys.add(11 + i);
            expectedKeys.add(51 + i);
        }
        Set<Integer> actualKeys = new HashSet<>();

        try(FileChannel ic = FileChannel.open(expectedFile.toPath(), StandardOpenOption.READ))
        {

            DataInputPlus.DataInputStreamPlus input = new DataInputPlus.DataInputStreamPlus(Channels.newInputStream(ic));
            try
            {

                FileMessageHeader header = FileMessageHeader.serializer.deserialize(input, MessagingService.current_version);
                Assert.assertTrue(header.isCompressed());
                StreamReader reader = new CompressedStreamReader(header, session) {
                    protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, UUID pendingRepair, SSTableFormat.Type format) throws IOException
                    {
                        return new NoopMultiWriter()
                        {

                            public boolean append(UnfilteredRowIterator partition)
                            {
                                actualKeys.add(partition.partitionKey().getKey().getInt());
                                while (partition.hasNext())
                                    partition.next();
                                return true;
                            }
                        };
                    }
                };
                reader.read(ic);
            }
            finally
            {
                input.close();
            }
        }

        Assert.assertEquals(expectedKeys, actualKeys);
    }
}
