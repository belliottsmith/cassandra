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

package org.apache.cassandra.service;


import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;


import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.cassandra.exceptions.UnavailableException;

import static org.apache.cassandra.db.ConsistencyLevel.*;
import static org.junit.Assert.fail;

public class WriteResponseHandlerTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static List<InetAddress> targets;
    static EnumSet<ConsistencyLevel> writeCls = EnumSet.allOf(ConsistencyLevel.class);
    static
    {
        writeCls.remove(SERIAL);
        writeCls.remove(LOCAL_SERIAL);
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddress.getByName("127.1.0.255"));
        metadata.updateHostId(UUID.randomUUID(), InetAddress.getByName("127.2.0.255"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddress endpoint)
            {
                return null;
            }

            public String getDatacenter(InetAddress endpoint)
            {
                byte[] address = endpoint.getAddress();
                if (address[1] == 1)
                    return "datacenter1";
                else
                    return "datacenter2";
            }

            public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress)
            {
                return null;
            }

            public void sortByProximity(InetAddress address, List<InetAddress> addresses)
            {

            }

            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
            {
                return false;
            }
        });
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        targets = ImmutableList.of(InetAddress.getByName("127.1.0.255"), InetAddress.getByName("127.1.0.254"), InetAddress.getByName("127.1.0.253"),
                                   InetAddress.getByName("127.2.0.255"), InetAddress.getByName("127.2.0.254"), InetAddress.getByName("127.2.0.253"));
    }

    @Before
    public void resetCounters()
    {
        ks.metric.writeFailedIdealCL.dec(ks.metric.writeFailedIdealCL.getCount());
    }


    /**
     * Validate that a successful write at ideal CL logs latency information. Also validates
     * DatacenterSyncWriteResponseHandler
     * @throws Throwable
     */
    @Test
    public void idealCLLatencyTracked() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        //Specify query start time in past to ensure minimum latency measurement
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        Thread.sleep(500);

        //dc1
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        //dc2
        awr.response(createDummyMessage(4));
        awr.response(createDummyMessage(5));

        //Don't need the others
        awr.expired();
        awr.expired();

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertTrue( TimeUnit.MILLISECONDS.toMicros(500) < ks.metric.idealCLWriteLatency.totalLatency.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that WriteResponseHandler does the right thing on success.
     * @throws Throwable
     */
    @Test
    public void idealCLWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ALL);

        //dc1
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        awr.response(createDummyMessage(2));
        //dc2
        awr.response(createDummyMessage(3));
        awr.response(createDummyMessage(4));
        awr.response(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that DatacenterWriteResponseHandler does the right thing on success.
     * @throws Throwable
     */
    @Test
    public void idealCLDatacenterWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM);

        //dc1
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        awr.response(createDummyMessage(2));
        //dc2
        awr.response(createDummyMessage(3));
        awr.response(createDummyMessage(4));
        awr.response(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that failing to achieve ideal CL increments the failure counter
     * @throws Throwable
     */
    @Test
    public void failedIdealCLIncrementsStat() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        //Succeed in local DC
        awr.response(createDummyMessage(0));
        awr.response(createDummyMessage(1));
        awr.response(createDummyMessage(2));

        //Fail in remote DC
        awr.expired();
        awr.expired();
        awr.expired();
        assertEquals(1, ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    @Test
    public void verifyAvailabilityChecks() throws Exception
    {
        // natural endpoints models NTS with 2 DCs, each having RF=3
        String local1  = "127.1.0.255";
        String local2  = "127.1.0.254";
        String local3  = "127.1.0.253";
        String remote1 = "127.2.0.255";
        String remote2 = "127.2.0.254";
        String remote3 = "127.2.0.253";
        Collection<InetAddress> natural = toAddresses(local1, local2, local3, remote1, remote2, remote3);

        // These addresses will be used for pending endpoints
        String local4  = "127.1.0.252";
        String local5  = "127.1.0.251";
        String local6  = "127.1.0.250";
        String remote4 = "127.2.0.252";
        String remote5 = "127.2.0.251";
        String remote6 = "127.2.0.250";

        // All replicas up & no pending endpoints
        verifyConsistencyLevels(natural, pending(), failureDetector());

        // All replicas up and 1 pending endpoint
        verifyConsistencyLevels(natural, pending(local4), failureDetector());

        // All replicas up and 1 pending endpoint per-dc
        verifyConsistencyLevels(natural, pending(local4, remote4), failureDetector());

        // One replica down in the local DC and no pending endpoints
        Predicate<InetAddress> failureDetector = failureDetector(local1);
        // ALL should fail, everything else should succeed
        verifyConsistencyLevels(natural, pending(), failureDetector, ALL);

        // One replica down and 1 pending node in the same DC
        // Expect same as previous, ALL should throw UnavailableException, everything else should succeed
        verifyConsistencyLevels(natural, pending(local4), failureDetector, ALL);

        // Two replicas down in the same DC and none pending
        // Expect ALL, LOCAL_QUORUM & EACH_QUORUM to throw UnavailableException
        failureDetector = failureDetector(local1, local2);
        verifyConsistencyLevels(natural, pending(), failureDetector, ALL, LOCAL_QUORUM, EACH_QUORUM);
        // Add a pending replica in the local DC. Expectation remains the same as the pending endpoint increases
        // the required endpoints for the consistency level. So a local quorum now requires 3 -> (rf=3/2 + 1) + 1
        verifyConsistencyLevels(natural, pending(local4), failureDetector, ALL, LOCAL_QUORUM, EACH_QUORUM);

        // All replicas in the local DC down, without any pending
        // ANY & non-local ONE/TWO/THREE should succeed, everything else (i.e. ALL/LOCAL_QUORUM/EACH_QUORUM/
        // LOCAL_ONE/QUORUM) should throw UnavailableException
        failureDetector = failureDetector(local1, local2, local3);
        verifyConsistencyLevels(natural, pending(), failureDetector, allWriteCLsExcept(ONE, TWO, THREE, ANY));

        // Add one pending replica, results should be unchanged
        verifyConsistencyLevels(natural, pending(local4), failureDetector, allWriteCLsExcept(ONE, TWO, THREE, ANY));

        // Add 2 more pending replicas, results should still be unchanged
        verifyConsistencyLevels(natural, pending(local4, local5, local6), failureDetector, allWriteCLsExcept(ONE, TWO, THREE, ANY));

        // All replicas in both DCs are down. Everything except ANY should throw UnavailableException
        failureDetector = failureDetector(local1, local2, local3, remote1, remote2, remote3);
        verifyConsistencyLevels(natural, pending(), failureDetector, allWriteCLsExcept(ANY));

        // Add 3 pending endpoints in each dc, everything except ANY should still fail
        String[] allPending = new String[]{local4, local5, local6, remote4, remote5, remote6};
        verifyConsistencyLevels(natural, pending(allPending), failureDetector, allWriteCLsExcept(ANY));
    }

    private static void verifyConsistencyLevels(Collection<InetAddress> naturalEndpoints,
                                                Collection<InetAddress> pendingEndpoints,
                                                Predicate<InetAddress> failureDetector,
                                                ConsistencyLevel...expectedToFail)
    {
        EnumSet<ConsistencyLevel> expectedSuccesses = EnumSet.copyOf(writeCls);
        EnumSet<ConsistencyLevel> expectedFailures = EnumSet.noneOf(ConsistencyLevel.class);
        for (ConsistencyLevel cl : expectedToFail)
        {
            expectedSuccesses.remove(cl);
            expectedFailures.add(cl);
        }

        Set<ConsistencyLevel> unexpectedSuccesses = EnumSet.noneOf(ConsistencyLevel.class);
        Set<ConsistencyLevel> unexpectedFailures = EnumSet.noneOf(ConsistencyLevel.class);

        for (ConsistencyLevel cl : writeCls)
        {
            for (WriteType type : WriteType.values())
            {
                AbstractWriteResponseHandler handler = createWriteResponseHandler(cl, naturalEndpoints, pendingEndpoints, WriteType.SIMPLE, failureDetector);
                try
                {
                    handler.assureSufficientLiveNodes();
                    if (!expectedSuccesses.contains(cl))
                        unexpectedSuccesses.add(cl);
                }
                catch (UnavailableException e)
                {
                    if (!expectedFailures.contains(cl))
                        unexpectedFailures.add(cl);
                }
            }
        }

        if (!unexpectedSuccesses.isEmpty() || !unexpectedFailures.isEmpty())
        {
            fail(String.format("Expectations not matched. Unexpected failures : %s, Unexpected successes: %s",
                               unexpectedFailures.stream().map(ConsistencyLevel::name).collect(Collectors.joining(", ")),
                               unexpectedSuccesses.stream().map(ConsistencyLevel::name).collect(Collectors.joining(", "))));
        }
    }

    private static ConsistencyLevel[] allWriteCLsExcept(ConsistencyLevel...exclude)
    {
        EnumSet<ConsistencyLevel> cls = EnumSet.copyOf(writeCls);
        for (ConsistencyLevel ex: exclude)
            cls.remove(ex);
        return cls.toArray(new ConsistencyLevel[cls.size()]);
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl,
                                                                           Collection<InetAddress> naturalEndpoints,
                                                                           Collection<InetAddress> pendingEndpoints,
                                                                           WriteType type,
                                                                           Predicate<InetAddress> isAlive)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(naturalEndpoints,
                                                                   pendingEndpoints,
                                                                   cl,
                                                                   () -> {},
                                                                   type,
                                                                   isAlive);
    }

    private Collection<InetAddress> pending(final String...endpoints) throws UnknownHostException
    {
        return toAddresses(endpoints);
    }

    private Collection<InetAddress> toAddresses(final String...endpoints) throws UnknownHostException
    {
        List<InetAddress> pending = Lists.newArrayListWithCapacity(endpoints.length);
        for (String ep : endpoints)
            pending.add(InetAddress.getByName(ep));
        return pending;
    }

    private Predicate<InetAddress> failureDetector(final String...deadNodes) throws UnknownHostException
    {
        Set<InetAddress> down = Sets.newHashSetWithExpectedSize(deadNodes.length);
        for (String deadNode : deadNodes)
            down.add(InetAddress.getByName(deadNode));

        return inetAddress -> !down.contains(inetAddress);
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(targets, ImmutableList.of(), cl, new Runnable() {
            public void run()
            {

            }
        }, WriteType.SIMPLE, ideal);
    }

    private static MessageIn createDummyMessage(int target)
    {
        return MessageIn.create(targets.get(target), null, null,  null, 0);
    }
}
