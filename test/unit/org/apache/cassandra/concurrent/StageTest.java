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

package org.apache.cassandra.concurrent;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class StageTest
{

    @Test
    public void fromPoolName()
    {
        // Do some extra case-insensitivity testing here, skip for the rest
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("read"));
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("READ"));
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("Read"));
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("readstage"));
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("READSTAGE"));
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("ReadStage"));
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("concurrent_reads"));
        // weird, but possible
        Assert.assertEquals(Stage.READ, Stage.fromPoolName("concurrent_readsstage"));

        Assert.assertEquals(Stage.MUTATION, Stage.fromPoolName("Mutation"));
        Assert.assertEquals(Stage.MUTATION, Stage.fromPoolName("MutationStage"));
        Assert.assertEquals(Stage.MUTATION, Stage.fromPoolName("concurrent_writers"));
        Assert.assertEquals(Stage.MUTATION, Stage.fromPoolName("concurrent_writersstage"));

        Assert.assertEquals(Stage.COUNTER_MUTATION, Stage.fromPoolName("Counter_Mutation"));
        Assert.assertEquals(Stage.COUNTER_MUTATION, Stage.fromPoolName("Counter_MutationStage"));
        Assert.assertEquals(Stage.COUNTER_MUTATION, Stage.fromPoolName("concurrent_counter_writes"));
        Assert.assertEquals(Stage.COUNTER_MUTATION, Stage.fromPoolName("concurrent_counter_writesstage"));

        Assert.assertEquals(Stage.VIEW_MUTATION, Stage.fromPoolName("View_Mutation"));
        Assert.assertEquals(Stage.VIEW_MUTATION, Stage.fromPoolName("View_MutationStage"));
        Assert.assertEquals(Stage.VIEW_MUTATION, Stage.fromPoolName("concurrent_materialized_view_writes"));
        Assert.assertEquals(Stage.VIEW_MUTATION, Stage.fromPoolName("concurrent_materialized_view_writesstage"));

        Assert.assertEquals(Stage.GOSSIP, Stage.fromPoolName("GOSSIP"));
        Assert.assertEquals(Stage.GOSSIP, Stage.fromPoolName("gossipstage"));

        Assert.assertEquals(Stage.REQUEST_RESPONSE, Stage.fromPoolName("REQUEST_RESPONSE"));
        Assert.assertEquals(Stage.REQUEST_RESPONSE, Stage.fromPoolName("request_responsestage"));

        Assert.assertEquals(Stage.ANTI_ENTROPY, Stage.fromPoolName("ANTI_ENTROPY"));
        Assert.assertEquals(Stage.ANTI_ENTROPY, Stage.fromPoolName("anti_entropystage"));

        Assert.assertEquals(Stage.MIGRATION, Stage.fromPoolName("MIGRATION"));
        Assert.assertEquals(Stage.MIGRATION, Stage.fromPoolName("migrationstage"));

        Assert.assertEquals(Stage.MISC, Stage.fromPoolName("MISC"));
        Assert.assertEquals(Stage.MISC, Stage.fromPoolName("miscstage"));

        Assert.assertEquals(Stage.TRACING, Stage.fromPoolName("TRACING"));
        Assert.assertEquals(Stage.TRACING, Stage.fromPoolName("tracingstage"));

        Assert.assertEquals(Stage.INTERNAL_RESPONSE, Stage.fromPoolName("INTERNAL_RESPONSE"));
        Assert.assertEquals(Stage.INTERNAL_RESPONSE, Stage.fromPoolName("internal_responsestage"));

        Assert.assertEquals(Stage.READ_REPAIR, Stage.fromPoolName("READ_REPAIR"));
        Assert.assertEquals(Stage.READ_REPAIR, Stage.fromPoolName("read_repairstage"));

    }
}