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

package org.apache.cassandra.schema;

import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;

public class SchemaTransformationsTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testUpdateKeyspacesAddNewTable()
    {
        String updatedKs = "system_auth";
        String newTableName = "roles2";
        TableId newTableId = TableId.fromString("00000000-0000-0000-0000-000000000000");

        // Create a version of the keyspace metadata with a new table
        KeyspaceMetadata km = AuthKeyspace.metadata();
        TableMetadata newTable = TableMetadata.builder(updatedKs, newTableName, newTableId)
                                                 .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                 .build();
        Tables updatedTables = km.tables.with(newTable);
        km = km.withSwapped(updatedTables);

        Assert.assertEquals(updatedKs, km.name);
        Assert.assertEquals(7, km.tables.size());

        // Transform with the in-memory Keyspace metadata
        SchemaTransformation transformation = SchemaTransformations.updateSystemKeyspace(AuthKeyspace.metadata(), AuthKeyspace.GENERATION);
        Keyspaces afterTransformation = transformation.apply(Keyspaces.of(km));

        Assert.assertEquals(1, afterTransformation.size());
        KeyspaceMetadata afterKs = afterTransformation.get(updatedKs).get();
        Assert.assertEquals(7, afterKs.tables.size());
        TableMetadata afterTable = afterKs.tables.getNullable(newTableName);

        // Confirm that new table was created
        Assert.assertEquals(newTableId, afterTable.id);
    }

    @Test
    public void testUpdateKeyspacesOnlyIdDiffers()
    {
        String updatedKs = "system_auth";
        String updatedTable = "roles";
        TableId originalId = TableId.fromString("00000000-0000-0000-0000-000000000000");

        // Create a version of the keyspace metadata with a non-deterministic ID
        KeyspaceMetadata km = AuthKeyspace.metadata();
        TableMetadata tm = km.tables.getNullable(updatedTable);
        Tables updatedTables = km.tables.without(tm).with(tm.unbuild().id(originalId).build());
        km = km.withSwapped(updatedTables);

        Assert.assertEquals(updatedKs, km.name);
        Assert.assertEquals(6, km.tables.size());
        Assert.assertEquals(originalId, km.tables.get(updatedTable).get().id);

        // Transform with the in-memory Keyspace metadata
        SchemaTransformation transformation = SchemaTransformations.updateSystemKeyspace(AuthKeyspace.metadata(), AuthKeyspace.GENERATION);
        Keyspaces afterTransformation = transformation.apply(Keyspaces.of(km));

        Assert.assertEquals(1, afterTransformation.size());
        KeyspaceMetadata afterKs = afterTransformation.get(updatedKs).get();
        Assert.assertEquals(6, afterKs.tables.size());
        TableMetadata afterTable = afterKs.tables.getNullable(updatedTable);

        // Confirm that the original ID was not modified
        Assert.assertEquals(originalId, afterTable.id);
    }

    @Test
    public void testUpdateKeyspacesIdAndColumnsDiffer()
    {
        String updatedKs = "system_auth";
        String updatedTableName = "roles";
        String newColumnName = "another";
        AbstractType<?> newColumnType = UTF8Type.instance;
        TableId originalId = TableId.fromString("00000000-0000-0000-0000-000000000000");

        // Create a version of the keyspace metadata with a non-deterministic ID
        Supplier<KeyspaceMetadata> originalKeyspaceSupplier = () -> {
            KeyspaceMetadata km = AuthKeyspace.metadata();
            TableMetadata tm = km.tables.getNullable(updatedTableName);
            TableMetadata updatedTm = tm.unbuild().id(originalId).build();
            Tables updatedTables = km.tables.without(tm).with(updatedTm);
            return km.withSwapped(updatedTables);
        };
        KeyspaceMetadata originalKeyspace = originalKeyspaceSupplier.get();

        Assert.assertEquals(updatedKs, originalKeyspace.name);
        Assert.assertEquals(6, originalKeyspace.tables.size());
        Assert.assertEquals(originalId, originalKeyspace.tables.get(updatedTableName).get().id);
        // New column should not exist yet
        Assert.assertFalse(originalKeyspace.tables.get(updatedTableName).get().regularColumns().contains(ColumnMetadata.regularColumn(updatedTableName, updatedTableName, newColumnName, newColumnType)));

        // Update to include an additional column
        Supplier<KeyspaceMetadata> keyspaceWithNewColumnSupplier = () -> {
            KeyspaceMetadata km = AuthKeyspace.metadata();
            TableMetadata tm = km.tables.getNullable(updatedTableName);

            TableMetadata updatedTm = tm.unbuild().id(originalId).addRegularColumn(newColumnName, newColumnType).build();

            Tables updatedTables = km.tables.without(tm).with(updatedTm);
            return km.withSwapped(updatedTables);
        };
        KeyspaceMetadata keyspaceWithNewColumn = keyspaceWithNewColumnSupplier.get();

        long updatedGeneration = AuthKeyspace.GENERATION + 1;

        // Transform with new column
        SchemaTransformation transformation = SchemaTransformations.updateSystemKeyspace(keyspaceWithNewColumn, updatedGeneration);
        Keyspaces afterTransformation = transformation.apply(Keyspaces.of(originalKeyspace));

        Assert.assertEquals(1, afterTransformation.size());
        KeyspaceMetadata afterKs = afterTransformation.get(updatedKs).get();
        Assert.assertEquals(6, afterKs.tables.size());
        TableMetadata afterTable = afterKs.tables.getNullable(updatedTableName);

        // Confirm that the original ID was not modified and the new column is present
        Assert.assertEquals(originalId, afterTable.id);
        Optional<ColumnMetadata> newColumn = afterTable.columns().stream().filter(cm -> cm.name.toString().equals(newColumnName)).findAny();
        Assert.assertEquals(newColumnName, newColumn.get().name.toString());
        Assert.assertEquals(newColumnType, newColumn.get().type);
    }
}