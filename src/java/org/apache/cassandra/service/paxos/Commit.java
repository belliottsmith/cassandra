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
import java.util.function.BiFunction;

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

import static org.apache.cassandra.service.paxos.Commit.CompareResult.AFTER;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.BEFORE;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.IS_REPROPOSAL;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.WAS_REPROPOSED_BY;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.SAME;

public class Commit
{
    enum CompareResult { SAME, BEFORE, AFTER, IS_REPROPOSAL, WAS_REPROPOSED_BY}

    public static final CommitSerializer<Commit> serializer = new CommitSerializer<>(Commit::new);

    public static class Proposal extends Commit
    {
        public static final CommitSerializer<Proposal> serializer = new CommitSerializer<>(Proposal::new);

        public Proposal(UUID ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public String toString()
        {
            return toString("Proposal");
        }

        public static Proposal from(UUID ballot, PartitionUpdate update)
        {
            update.updateAllTimestamp(UUIDGen.microsTimestamp(ballot));
            return new Proposal(ballot, update);
        }

        public static Proposal empty(UUID ballot, DecoratedKey partitionKey, CFMetaData metadata)
        {
            return new Proposal(ballot, PartitionUpdate.emptyUpdate(metadata, partitionKey));
        }

        public Agreed agreed()
        {
            return new Agreed(ballot, update);
        }
    }

    public static class Accepted extends Proposal
    {
        public static final CommitSerializer<Accepted> serializer = new CommitSerializer<>(Accepted::new);

        public static Accepted none(DecoratedKey partitionKey, CFMetaData metadata)
        {
            return new Accepted(Ballot.none(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
        }

        public Accepted(UUID ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public String toString()
        {
            return toString("Accepted");
        }
    }

    // might prefer to call this Commit, but would mean refactoring more legacy code
    public static class Agreed extends Commit
    {
        public static final CommitSerializer<Agreed> serializer = new CommitSerializer<>(Agreed::new);

        public Agreed(UUID ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }
    }

    public static class Committed extends Agreed
    {
        public static final CommitSerializer<Committed> serializer = new CommitSerializer<>(Committed::new);

        public static Committed none(DecoratedKey partitionKey, CFMetaData metadata)
        {
            return new Committed(Ballot.none(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
        }

        public Committed(UUID ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public String toString()
        {
            return toString("Committed");
        }
    }

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

    public static Commit emptyCommit(DecoratedKey partitionKey, CFMetaData metadata)
    {
        return new Commit(Ballot.none(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
    }

    @Deprecated
    public static Commit newProposal(UUID ballot, PartitionUpdate update)
    {
        update.updateAllTimestamp(UUIDGen.microsTimestamp(ballot));
        return new Commit(ballot, update);
    }

    public boolean isAfter(Commit other)
    {
        return other == null || ballot.timestamp() > other.ballot.timestamp();
    }

    public boolean isAfter(@Nullable UUID otherBallot)
    {
        return otherBallot == null || ballot.timestamp() > otherBallot.timestamp();
    }

    public boolean isBefore(@Nullable UUID otherBallot)
    {
        return otherBallot != null && ballot.timestamp() < otherBallot.timestamp();
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
        return toString("Commit");
    }

    public String toString(String kind)
    {
        return String.format("%s(%d:%s, %d:%s)", kind, ballot.timestamp(), ballot, update.stats().minTimestamp, update.toString(false));
    }

    /**
     * We can witness reproposals of the latest successful commit; we can detect this by comparing the timestamp of
     * the update with our ballot; if it is the same, we are not a reproposal. If it is the same as either the
     * ballot timestamp or update timestamp of the latest committed proposal, then we are reproposing it and can
     * instead simpy commit it.
     */
    public boolean isReproposalOf(Commit older)
    {
        return isReproposal(older, older.ballot.timestamp(), this, this.ballot.timestamp());
    }

    private boolean isReproposal(Commit older, long ballotOfOlder, Commit newer, long ballotOfNewer)
    {
        // NOTE: it would in theory be possible to just check
        // newer.update.stats().minTimestamp == older.update.stats().minTimestamp
        // however this could be brittle, if for some reason they don't get updated;
        // the logic below is fail-safe, in that if minTimestamp is not set we will treat it as not a reproposal
        // which is the safer way to get it wrong.

        // the timestamp of a mutation stays unchanged as we repropose it, so the timestamp of the mutation
        // is the timestamp of the ballot that originally proposed it
        long originalBallotOfNewer = newer.update.stats().minTimestamp;

        // so, if the mutation and ballot timestamps match, this is not a reproposal but a first proposal
        if (ballotOfNewer == originalBallotOfNewer)
            return false;

        // otherwise, if the original proposing ballot matches the older proposal's ballot, it is reproposing it
        if (originalBallotOfNewer == ballotOfOlder)
            return true;

        // otherwise, it could be that both are reproposals, so just check both for the "original" ballot timestamp
        return originalBallotOfNewer == older.update.stats().minTimestamp;
    }

    public CompareResult compareWith(Commit that)
    {
        long thisBallot = this.ballot.timestamp();
        long thatBallot = that.ballot.timestamp();
        // by the time we reach proposal and commit, timestamps are unique so we can assert identity
        if (thisBallot == thatBallot)
            return SAME;

        if (thisBallot < thatBallot)
            return isReproposal(this, thisBallot, that, thatBallot) ? WAS_REPROPOSED_BY : BEFORE;
        else
            return isReproposal(that, thatBallot, this, thisBallot) ? IS_REPROPOSAL : AFTER;
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

    public static class CommitSerializer<T extends Commit> implements IVersionedSerializer<T>
    {
        final BiFunction<UUID, PartitionUpdate, T> constructor;
        public CommitSerializer(BiFunction<UUID, PartitionUpdate, T> constructor)
        {
            this.constructor = constructor;
        }

        public void serialize(T commit, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                ByteBufferUtil.writeWithShortLength(commit.update.partitionKey().getKey(), out);

            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            PartitionUpdate.serializer.serialize(commit.update, out, version);
        }

        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer partitionKey = null;
            if (version < MessagingService.VERSION_30)
                partitionKey = ByteBufferUtil.readWithShortLength(in);

            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL, partitionKey);
            return constructor.apply(ballot, update);
        }

        public long serializedSize(T commit, int version)
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
