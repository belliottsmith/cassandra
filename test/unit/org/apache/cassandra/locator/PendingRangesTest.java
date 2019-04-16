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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PendingRangesTest
{
    private static final Logger logger = LoggerFactory.getLogger(PendingRangesTest.class);

    private static final String RACK1 = "RACK1";
    private static final String DC1 = "DC1";
    private static final String KEYSPACE = "ks";
    private static final InetAddress PEER1 = peer(1);
    private static final InetAddress PEER2 = peer(2);
    private static final InetAddress PEER3 = peer(3);
    private static final InetAddress PEER4 = peer(4);
    private static final InetAddress PEER5 = peer(5);
    private static final InetAddress PEER6 = peer(6);

    private static final InetAddress PEER1A = peer(11);
    private static final InetAddress PEER4A = peer(14);

    private static final Token TOKEN1 = token(0);
    private static final Token TOKEN2 = token(10);
    private static final Token TOKEN3 = token(20);
    private static final Token TOKEN4 = token(30);
    private static final Token TOKEN5 = token(40);
    private static final Token TOKEN6 = token(50);

    @Test
    public void calculatePendingRangesForConcurrentReplacements()
    {
        /*
         * As described in CASSANDRA-14802, concurrent range movements can generate pending ranges
         * which are far larger than strictly required, which in turn can impact availability.
         *
         * In the narrow case of straight replacement, the pending ranges should mirror the owned ranges
         * of the nodes being replaced.
         * E.g. a 6 node cluster with tokens:
         *
         * nodeA : 0
         * nodeB : 10
         * nodeC : 20
         * nodeD : 30
         * nodeE : 40
         * nodeF : 50
         *
         * with an RF of 3, this gives an initial ring of :
         *
         * nodeA : (50, 0], (40, 50], (30, 40]
         * nodeB : (0, 10], (50, 0], (40, 50]
         * nodeC : (10, 20], (0, 10], (50, 0]
         * nodeD : (20, 30], (10, 20], (0, 10]
         * nodeE : (30, 40], (20, 30], (10, 20]
         * nodeF : (40, 50], (30, 40], (20, 30]
         *
         * If nodeA is replaced by node1A, then the pending ranges map should be:
         * {
         *   (50, 0]  : [node1A],
         *   (40, 50] : [node1A],
         *   (30, 40] : [node1A]
         * }
         *
         * Starting a second concurrent replacement of a node with non-overlapping ranges
         * (i.e. node4 for node4A) should result in a pending range map of:
         * {
         *   (50, 0]  : [node1A],
         *   (40, 50] : [node1A],
         *   (30, 40] : [node1A],
         *   (20, 30] : [node4A],
         *   (10, 20] : [node4A],
         *   (0, 10]  : [node4A]
         * }
         *
         * But, the bug in CASSANDRA-14802 causes it to be:
         * {
         *   (50, 0]  : [node1A],
         *   (40, 50] : [node1A],
         *   (30, 40] : [node1A],
         *   (20, 30] : [node4A],
         *   (10, 20] : [node4A],
         *   (50, 10] : [node4A]
         * }
         *
         * so node4A incorrectly becomes a pending endpoint for an additional sub-range: (50, 0)
         */
        TokenMetadata tm = new TokenMetadata();
        AbstractReplicationStrategy replicationStrategy = simpleStrategy(tm, 3);

        // setup initial ring
        addNode(tm, PEER1, TOKEN1);
        addNode(tm, PEER2, TOKEN2);
        addNode(tm, PEER3, TOKEN3);
        addNode(tm, PEER4, TOKEN4);
        addNode(tm, PEER5, TOKEN5);
        addNode(tm, PEER6, TOKEN6);

        // no pending ranges before any replacements
        tm.calculatePendingRanges(replicationStrategy, KEYSPACE);
        assertEquals(0, Iterators.size(tm.getPendingRanges(KEYSPACE).iterator()));

        // Ranges initially owned by PEER1 and PEER4
        Collection<Range<Token>> peer1Ranges = replicationStrategy.getAddressRanges(tm).get(PEER1);
        Collection<Range<Token>> peer4Ranges = replicationStrategy.getAddressRanges(tm).get(PEER4);

        // Replace PEER1 with PEER1A
        replace(PEER1, PEER1A, TOKEN1, tm, replicationStrategy);
        // The only pending ranges should be the ones previously belonging to PEER1
        // and these should have a single pending endpoint, PEER1A
        Map<InetAddress, Collection<Range<Token>>> expectedPendingByEndpoint = ImmutableMap.of(PEER1A, peer1Ranges);
        assertPendingRanges(tm.getPendingRanges(KEYSPACE), Collections.singletonMap(PEER1A, peer1Ranges));
        // Also verify the Multimap variant of getPendingRanges
        assertPendingRangesMM(tm.getPendingRangesMM(KEYSPACE), expectedPendingByEndpoint);

        // Replace PEER4 with PEER4A
        replace(PEER4, PEER4A, TOKEN4, tm, replicationStrategy);
        // Pending ranges should now include the ranges originally belonging
        // to PEER1 (now pending for PEER1A) and the ranges originally belonging to PEER4
        // (now pending for PEER4A).
        expectedPendingByEndpoint = ImmutableMap.of(PEER1A, peer1Ranges,
                                                    PEER4A, peer4Ranges);
        assertPendingRanges(tm.getPendingRanges(KEYSPACE), expectedPendingByEndpoint);
        assertPendingRangesMM(tm.getPendingRangesMM(KEYSPACE), expectedPendingByEndpoint);
    }

    private void assertPendingRanges(PendingRangeMaps pending,
                                     Map<InetAddress, Collection<Range<Token>>> expectedPendingPerEndpoint)
    {
        Map<InetAddress, Set<Range<Token>>> pendingRangeSets = Maps.newHashMapWithExpectedSize(expectedPendingPerEndpoint.size());
        expectedPendingPerEndpoint.forEach((endpoint, rangeCollection) -> pendingRangeSets.put(endpoint, Sets.newHashSet(rangeCollection)));

        pending.iterator().forEachRemaining(pendingRange -> {
            InetAddress endpoint = Iterators.getOnlyElement(pendingRange.getValue().iterator());
            assertTrue(pendingRangeSets.containsKey(endpoint));
            assertTrue(pendingRangeSets.get(endpoint).remove(pendingRange.getKey()));
        });
        pendingRangeSets.forEach((endpoint, ranges) -> assertTrue(ranges.isEmpty()));
    }

    private void assertPendingRangesMM(Multimap<Range<Token>, InetAddress> pendingMM,
                                       Map<InetAddress, Collection<Range<Token>>> expectedPendingPerEndpoint)
    {
        Map<InetAddress, Set<Range<Token>>> pendingRangeSets = Maps.newHashMapWithExpectedSize(expectedPendingPerEndpoint.size());
        expectedPendingPerEndpoint.forEach((endpoint, rangeCollection) -> pendingRangeSets.put(endpoint, Sets.newHashSet(rangeCollection)));

        pendingMM.entries().forEach(entry -> {
            InetAddress endpoint = entry.getValue();
            assertTrue(pendingRangeSets.containsKey(endpoint));
            assertTrue(pendingRangeSets.get(endpoint).remove(entry.getKey()));
        });
        pendingRangeSets.forEach((endpoint, ranges) -> assertTrue(ranges.isEmpty()));
    }

    private void addNode(TokenMetadata tm, InetAddress endpoint, Token token)
    {
        tm.updateNormalTokens(Collections.singleton(token), endpoint);
    }

    private void replace(InetAddress toReplace,
                         InetAddress replacement,
                         Token token,
                         TokenMetadata tm,
                         AbstractReplicationStrategy replicationStrategy)
    {
        assertEquals(toReplace, tm.getEndpoint(token));
        tm.addReplaceTokens(Collections.singleton(token), replacement, toReplace);
        tm.calculatePendingRanges(replicationStrategy, KEYSPACE);
    }

    private static Token token(long token)
    {
        return Murmur3Partitioner.instance.getTokenFactory().fromString(Long.toString(token));
    }

    private static InetAddress peer(int addressSuffix)
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static IEndpointSnitch snitch()
    {
        return new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            public String getDatacenter(InetAddress endpoint)
            {
                return DC1;
            }
        };
    }

    private static AbstractReplicationStrategy simpleStrategy(TokenMetadata tokenMetadata, int replicationFactor)
    {
        return new SimpleStrategy(KEYSPACE,
                                  tokenMetadata,
                                  snitch(),
                                  Collections.singletonMap("replication_factor", Integer.toString(replicationFactor)));
    }
}