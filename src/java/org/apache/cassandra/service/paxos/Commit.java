package org.apache.cassandra.service.paxos;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class Commit
{
    public static final CommitSerializer serializer = new CommitSerializer();
    private static final UUID emptyBallot = UUIDGen.getTimeUUID(0);

    public final UUID ballot;
    public final PartitionUpdate update;

    public Commit(UUID ballot, PartitionUpdate update)
    {
        assert ballot != null;
        assert update != null;

        this.ballot = ballot;
        this.update = update;
    }

    public static Commit newPrepare(DecoratedKey partitionKey, CFMetaData metadata, UUID ballot)
    {
        return new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, partitionKey));
    }

    public static Commit newProposal(UUID ballot, PartitionUpdate update)
    {
        update.updateAllTimestamp(UUIDGen.microsTimestamp(ballot));
        return new Commit(ballot, update);
    }

    public static Commit emptyCommit(DecoratedKey partitionKey, CFMetaData metadata)
    {
        return new Commit(emptyBallot(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
    }

    public static UUID emptyBallot()
    {
        return emptyBallot;
    }

    public boolean isAfter(Commit other)
    {
        return other == null || ballot.timestamp() > other.ballot.timestamp();
    }

    public boolean isAfter(@Nullable UUID otherBallot)
    {
        return otherBallot == null || ballot.timestamp() > otherBallot.timestamp();
    }

    public boolean hasBallot(UUID ballot)
    {
        return this.ballot.equals(ballot);
    }

    public boolean hasSameBallot(Commit other)
    {
        return this.ballot.equals(other.ballot);
    }

    public Mutation makeMutation()
    {
        return new Mutation(update);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Commit commit = (Commit) o;

        return ballot.equals(commit.ballot) && update.equals(commit.update);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ballot, update);
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s)", ballot, update);
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable Commit testIsAfter, @Nullable Commit testIsBefore)
    {
        return testIsAfter != null && testIsAfter.isAfter(testIsBefore);
    }

    /**
     * True iff a and b are both not null and a.hasSameBallot(b)
     */
    public static boolean hasSameBallot(@Nullable Commit a, @Nullable Commit b)
    {
        return a != null && b != null && a.hasSameBallot(b);
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable UUID testIsAfter, @Nullable Commit testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.timestamp() > testIsBefore.ballot.timestamp());
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable Commit testIsAfter, @Nullable UUID testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.ballot.timestamp() > testIsBefore.timestamp());
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable UUID testIsAfter, @Nullable UUID testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.timestamp() > testIsBefore.timestamp());
    }

    public static class CommitSerializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit commit, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                ByteBufferUtil.writeWithShortLength(commit.update.partitionKey().getKey(), out);

            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            PartitionUpdate.serializer.serialize(commit.update, out, version);
        }

        public Commit deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer partitionKey = null;
            if (version < MessagingService.VERSION_30)
                partitionKey = ByteBufferUtil.readWithShortLength(in);

            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL, partitionKey);
            return new Commit(ballot, update);
        }

        public long serializedSize(Commit commit, int version)
        {
            int size = 0;
            if (version < MessagingService.VERSION_30)
                size += ByteBufferUtil.serializedSizeWithShortLength(commit.update.partitionKey().getKey());

            return size
                 + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
                 + PartitionUpdate.serializer.serializedSize(commit.update, version);
        }
    }

}
