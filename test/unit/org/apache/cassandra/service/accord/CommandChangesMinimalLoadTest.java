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

package org.apache.cassandra.service.accord;

import org.assertj.core.api.SoftAssertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.DurableBefore;
import accord.local.MinimalCommand;
import accord.local.RedundantBefore;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Gen;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.journal.CommandChangeWriter;
import org.apache.cassandra.service.accord.journal.CommandChanges;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.api.Journal.Load;
import static accord.local.RedundantStatus.SomeStatus.NONE;
import static accord.utils.Property.qt;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

/**
 * A command record written by CommandChangeWriter must be readable with Load.MINIMAL and Load.MINIMAL_WITH_DEPS,
 * as AccordJournal.loadMinimal / loadMinimalWithDeps and the shrunk RangeIndex entry path decode it.
 */
public class CommandChangesMinimalLoadTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        TableMetadata tbl = Schema.instance.getTableMetadata("ks", "tbl");
        Assert.assertEquals(TransactionalMode.full, tbl.params.transactionalMode);
        StorageService.instance.initServer();
    }

    private static CommandChanges decode(Command orig, Load load, DataOutputBuffer out, Version version) throws Exception
    {
        out.clear();
        CommandChangeWriter writer = CommandChangeWriter.make(null, orig);
        if (writer == null)
            return null;
        writer.write(out, version);
        CommandChanges builder = new CommandChanges(orig.txnId(), load);
        builder.deserializeNext(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), version);
        return builder;
    }

    @Test
    public void minimalLoadDecodesSaveStatusAndParticipants()
    {
        Gen<AccordGenerators.CommandBuilder> gen = AccordGenerators.commandsBuilder();
        SaveStatus[] statuses = { SaveStatus.PreAccepted, SaveStatus.Committed, SaveStatus.Stable, SaveStatus.PreApplied };
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            qt().withExamples(10).forAll(gen).check(cmdBuilder -> {
                Version version = Version.LATEST;
                SoftAssertions checks = new SoftAssertions();
                for (SaveStatus saveStatus : statuses)
                {
                    Command orig = cmdBuilder.build(saveStatus);

                    // Control: the full load recovers the fields.
                    CommandChanges full = decode(orig, Load.ALL, out, version);
                    if (full == null)
                        continue;
                    checks.assertThat(full.saveStatus()).describedAs("ALL saveStatus for %s", saveStatus).isEqualTo(orig.saveStatus());

                    for (Load load : new Load[]{ Load.MINIMAL, Load.MINIMAL_WITH_DEPS })
                    {
                        CommandChanges builder = decode(orig, load, out, version);
                        checks.assertThat(builder.saveStatus())
                              .describedAs("%s decode of %s %s: saveStatus", load, orig.txnId(), saveStatus)
                              .isEqualTo(orig.saveStatus());
                        checks.assertThat(builder.participants())
                              .describedAs("%s decode of %s %s: participants", load, orig.txnId(), saveStatus)
                              .isEqualTo(orig.participants());

                        // AccordJournal.loadMinimal path: cleanupAndFilter must not drop a live command.
                        RedundantBefore owned = RedundantBefore.create(AccordTestUtils.fullRange(orig.partialTxn()), TxnId.NONE, NONE);
                        CommandChanges filtered = CommandChanges.cleanupAndFilter(builder, owned, DurableBefore.EMPTY);
                        checks.assertThat(filtered)
                              .describedAs("%s loadMinimal of live %s %s returned null (command dropped)", load, orig.txnId(), saveStatus)
                              .isNotNull();
                        if (filtered != null && load == Load.MINIMAL)
                        {
                            MinimalCommand minimal = filtered.asMinimal();
                            checks.assertThat(minimal.saveStatus).isEqualTo(orig.saveStatus());
                        }
                    }
                }
                checks.assertAll();
            });
        }
    }
}
