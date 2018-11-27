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

import com.google.common.io.Files;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class SerializationHeaderTest
{
    private static String KEYSPACE = "SerializationHeaderTest";

    public void testWrittenAsDifferentKind() throws Exception
    {
        final String tableName = "testWrittenAsDifferentKind";
        final String schemaCqlWithStatic = String.format("CREATE TABLE %s (k int PRIMARY KEY, v static int)", tableName);
        final String schemaCqlWithRegular = String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", tableName);
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        CFMetaData schemaWithStatic = CFMetaData.compile(schemaCqlWithStatic, KEYSPACE);
        CFMetaData schemaWithRegular = CFMetaData.compile(schemaCqlWithRegular, KEYSPACE);
        ColumnDefinition columnStatic = schemaWithStatic.getColumnDefinition(v);
        ColumnDefinition columnRegular = schemaWithRegular.getColumnDefinition(v);
        schemaWithStatic.recordColumnDrop(columnRegular, 0L);
        schemaWithRegular.recordColumnDrop(columnStatic, 0L);


        File dir = Files.createTempDir();
        try
        {
            Function<String, Callable<Descriptor>> writer = schemaCql -> () -> {
                try (CQLSSTableWriter sstableWriter = CQLSSTableWriter.builder()
                        .forTable(schemaCqlWithStatic)
                        .inDirectory(dir)
                        .withPartitioner(Murmur3Partitioner.instance)
                        .using(String.format("INSERT INTO %s.%s(k, v) VALUES(?, ?)", KEYSPACE, tableName))
                        .build())
                {
                    sstableWriter.addRow(1, 1);
                }
                return Arrays.stream(dir.listFiles())
                        .map(File::getName)
                        .map(Descriptor::fromFilename)
                        .sorted(Comparator.comparingInt(d -> -d.generation))
                        .findFirst()
                        .get();
            };
            Descriptor sstableWithStatic = writer.apply(schemaCqlWithStatic).call();
            Descriptor sstableWithRegular = writer.apply(schemaCqlWithRegular).call();
            SSTableReader readerWithStatic = SSTableReader.openNoValidation(sstableWithStatic, schemaWithRegular);
            SSTableReader readerWithRegular = SSTableReader.openNoValidation(sstableWithRegular, schemaWithStatic);

            try (ISSTableScanner scanStatic = readerWithStatic.getScanner()) {
                UnfilteredRowIterator iter = scanStatic.next();
                Assert.assertFalse(iter.hasNext());
                Assert.assertEquals(Int32Type.instance.compose(iter.staticRow().getCell(columnStatic).value()), 1L);
            }
        }
        finally
        {
            FileUtils.deleteRecursive(dir);
        }
    }

}
