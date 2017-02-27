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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.util.File;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class DescriptorTest
{
    private final String ksname = "ks";
    private final String cfname = "cf";
    private final String cfId = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(UUID.randomUUID()));
    private final File tempDataDir;

    public DescriptorTest() throws IOException
    {
        // create CF directories, one without CFID and one with it
        tempDataDir = FileUtils.createTempFile("DescriptorTest", null).parent();
    }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testFromFilename() throws Exception
    {
        File cfIdDir = new File(tempDataDir.absolutePath() + File.pathSeparator() + ksname + File.pathSeparator() + cfname + '-' + cfId);
        testFromFilenameFor(cfIdDir);
    }

    @Test
    public void testFromFilenameInBackup() throws Exception
    {
        File backupDir = new File(StringUtils.join(new String[]{ tempDataDir.absolutePath(), ksname, cfname + '-' + cfId, Directories.BACKUPS_SUBDIR}, File.pathSeparator()));
        testFromFilenameFor(backupDir);
    }

    @Test
    public void testFromFilenameInSnapshot() throws Exception
    {
        File snapshotDir = new File(StringUtils.join(new String[]{ tempDataDir.absolutePath(), ksname, cfname + '-' + cfId, Directories.SNAPSHOT_SUBDIR, "snapshot_name"}, File.pathSeparator()));
        testFromFilenameFor(snapshotDir);
    }

    @Test
    public void testFromFilenameInLegacyDirectory() throws Exception
    {
        File cfDir = new File(tempDataDir.absolutePath() + File.pathSeparator() + ksname + File.pathSeparator() + cfname);
        testFromFilenameFor(cfDir);
    }

    private void testFromFilenameFor(File dir)
    {
        checkFromFilename(new Descriptor(dir, ksname, cfname, new SequenceBasedSSTableId(1), SSTableFormat.Type.BIG));

        // secondary index
        String idxName = "myidx";
        File idxDir = new File(dir.absolutePath() + File.pathSeparator() + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName);
        checkFromFilename(new Descriptor(idxDir, ksname, cfname + Directories.SECONDARY_INDEX_NAME_SEPARATOR + idxName, new SequenceBasedSSTableId(4), SSTableFormat.Type.BIG));
    }

    private void checkFromFilename(Descriptor original)
    {
        File file = new File(original.filenameFor(Component.DATA));

        Pair<Descriptor, Component> pair = Descriptor.fromFilenameWithComponent(file);
        Descriptor desc = pair.left;

        assertEquals(original.directory, desc.directory);
        assertEquals(original.ksname, desc.ksname);
        assertEquals(original.cfname, desc.cfname);
        assertEquals(original.version, desc.version);
        assertEquals(original.id, desc.id);
        assertEquals(Component.DATA, pair.right);
    }

    @Test
    public void testEquality()
    {
        // Descriptor should be equal when parent directory points to the same directory
        File dir = new File(".");
        Descriptor desc1 = new Descriptor(dir, "ks", "cf", new SequenceBasedSSTableId(1), SSTableFormat.Type.BIG);
        Descriptor desc2 = new Descriptor(dir.toAbsolute(), "ks", "cf", new SequenceBasedSSTableId(1), SSTableFormat.Type.BIG);
        assertEquals(desc1, desc2);
        assertEquals(desc1.hashCode(), desc2.hashCode());
    }

    @Test
    public void validateNames()
    {
        String[] names = {
             "/data/keyspace1/standard1-8b7c7b23f1ad3f4554f8d64f85d9d3637661/keyspace1-standard1-na-1-big-Data.db",
             // 2ndary index
             "/data/keyspace1/standard1-8b7c7b23f1ad3f4554f8d64f85d9d3637661/.idx1" + File.pathSeparator() + "keyspace1-standard1.idx1-na-1-big-Data.db",
             // old ma-mc format
             "/data/keyspace1/standard1-8b7c7b23f1ad3f4554f8d64f85d9d3637661/ma-1-big-Data.db",
             "/data/keyspace1/standard1-8b7c7b23f1ad3f4554f8d64f85d9d3637661/mb-1-big-Data.db",
             "/data/keyspace1/standard1-8b7c7b23f1ad3f4554f8d64f85d9d3637661/mc-1-big-Data.db",
             // md is back to full length
             "/data/system_schema/keyspaces-8b7c7b23f1ad3f4554f8d64f85d9d3637661/system_schema-keyspaces-md-1-big-Data.db",
        };

        for (String name : names)
        {
            assertNotNull(Descriptor.fromFilename(name));
        }
    }

    @Test
    public void badNames()
    {
        String names[] = {
                "/data/system-schema/keyspaces/system-schema_keyspaces-k234a-1-CompressionInfo.db",
                "/data/system-schema/keyspaces/system-schema_keyspaces-ka-aa-Summary.db",
                "/data/system-schema/keyspaces/system-schema_keyspaces-XXX-ka-1-Data.db",
                "/data/system-schema/keyspaces/system-schema_keyspaces-k",
                "/data/system-schema/keyspaces/system-schema_keyspace-ka-1-AAA-Data.db",
                "/data/system-schema/keyspaces/system-schema-keyspace-ka-1-AAA-Data.db",
        };

        for (String name : names)
        {
            try
            {
                Descriptor d = Descriptor.fromFilename(name);
                Assert.fail(name);
            } catch (IllegalArgumentException e) {
                //good
            }
        }
    }

    @Test
    public void warningNames()
    {
        String names[] = {
        "/data/wrongkeyspace/standard1-8b7c7b23f1ad3f4554f8d64f85d9d3637661/keyspace1-standard1-ma-1-big-Data.db",
        "/data/keyspace1/wrongtable-8b7c7b23f1ad3f4554f8d64f85d9d3637661/keyspace1-standard1-ma-1-big-Data.db",
        };

        for (String name : names)
        {
            assertNotNull(Descriptor.fromFilename(name)); // should just generate a warning in the logs
        }
    }


}
