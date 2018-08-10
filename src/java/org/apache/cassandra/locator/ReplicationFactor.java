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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

// warning: equals() depends on class identity; if extending, be sure to assess this
public final class ReplicationFactor
{
    public static final ReplicationFactor ZERO = new ReplicationFactor(0);

    // all replicas, both transient and full
    public final int allReplicas;
    // full replicas only, i.e. those containing complete copies of the data for the replicated token range
    public final int fullReplicas;

    private ReplicationFactor(int allReplicas, int transientRF)
    {
        validate(allReplicas, transientRF);
        this.allReplicas = allReplicas;
        this.fullReplicas = allReplicas - transientRF;
    }

    public int transientReplicas()
    {
        return allReplicas - fullReplicas;
    }

    public boolean hasTransientReplicas()
    {
        return allReplicas != fullReplicas;
    }

    private ReplicationFactor(int allReplicas)
    {
        this(allReplicas, 0);
    }

    static void validate(int totalRF, int transientRF)
    {
        Preconditions.checkArgument(transientRF == 0 || DatabaseDescriptor.isTransientReplicationEnabled(),
                                    "Transient replication is not enabled on this node");
        Preconditions.checkArgument(totalRF >= 0,
                                    "Replication factor must be non-negative, found %s", totalRF);
        Preconditions.checkArgument(transientRF == 0 || transientRF < totalRF,
                                    "Transient replicas must be zero, or less than total replication factor. For %s/%s", totalRF, transientRF);
        if (transientRF > 0)
        {
            Preconditions.checkArgument(DatabaseDescriptor.getNumTokens() == 1,
                                        "Transient nodes are not allowed with multiple tokens");
            Stream<InetAddressAndPort> endpoints = Stream.concat(Gossiper.instance.getLiveMembers().stream(), Gossiper.instance.getUnreachableMembers().stream());
            List<InetAddressAndPort> badVersionEndpoints = endpoints.filter(Predicates.not(FBUtilities.getBroadcastAddressAndPort()::equals))
                                                                    .filter(endpoint -> Gossiper.instance.getReleaseVersion(endpoint) != null && Gossiper.instance.getReleaseVersion(endpoint).major < 4)
                                                                    .collect(Collectors.toList());
            if (!badVersionEndpoints.isEmpty())
                throw new AssertionError("Transient replication is not supported in mixed version clusters with nodes < 4.0. Bad nodes: " + badVersionEndpoints);
        }
        else if (transientRF < 0)
        {
            throw new AssertionError(String.format("Amount of transient nodes should be strictly positive, but was: '%d'", transientRF));
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationFactor that = (ReplicationFactor) o;
        return allReplicas == that.allReplicas && fullReplicas == that.fullReplicas;
    }

    public int hashCode()
    {
        return Objects.hash(allReplicas, fullReplicas);
    }

    public static ReplicationFactor rf(int totalRF)
    {
        return new ReplicationFactor(totalRF);
    }

    public static ReplicationFactor rf(int totalRF, int transientRF)
    {
        return new ReplicationFactor(totalRF, transientRF);
    }

    public static ReplicationFactor fromString(String s)
    {
        if (s.contains("/"))
        {
            String[] parts = s.split("/");
            Preconditions.checkArgument(parts.length == 2,
                                        "Replication factor format is <replicas> or <replicas>/<transient>");
            return new ReplicationFactor(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
        }
        else
        {
            return new ReplicationFactor(Integer.valueOf(s), 0);
        }
    }

    @Override
    public String toString()
    {
        return "rf(" + allReplicas + (hasTransientReplicas() ? '/' + transientReplicas() : "") + ')';
    }
}
