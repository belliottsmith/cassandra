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

package org.apache.cassandra.service.accord.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.Nonnull;

import com.google.common.primitives.Ints;

import accord.api.RoutingKey;
import accord.local.RedundantBefore;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.InternalStatus;
import accord.local.cfk.CommandsForKey.TxnInfoExtra;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.utils.vint.VIntCoding;

import static accord.local.cfk.CommandsForKey.NO_BOUNDS_INFO;
import static accord.local.cfk.CommandsForKey.NO_PENDING_UNMANAGED;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.SyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer.TxnIdFlags.RAW_BITS;
import static org.apache.cassandra.utils.ByteBufferUtil.readLeastSignificantBytes;
import static org.apache.cassandra.utils.ByteBufferUtil.writeLeastSignificantBytes;
import static org.apache.cassandra.utils.ByteBufferUtil.writeMostSignificantBytes;
import static org.apache.cassandra.utils.vint.VIntCoding.decodeZigZag64;
import static org.apache.cassandra.utils.vint.VIntCoding.encodeZigZag64;

public class CommandsForKeySerializer
{
    private static final int HAS_MISSING_DEPS_HEADER_BIT = 0x1;
    private static final int HAS_EXECUTE_AT_HEADER_BIT = 0x2;
    private static final int HAS_BALLOT_HEADER_BIT = 0x4;
    private static final int HAS_STATUS_OVERRIDES = 0x8;
    private static final int HAS_NON_STANDARD_FLAGS = 0x10;

    /**
     * We read/write a fixed number of intial bytes for each command, with an initial flexible number of flag bits
     * and the remainder interpreted as the HLC/epoch/node.
     *
     * The preamble encodes:
     *  vint32: number of commands
     *  vint32: number of unique node Ids
     *  [unique node ids]
     *  two flag bytes:
     *   bit 0 is set if there are any missing ids;
     *   bit 1 is set if there are any executeAt specified
     *   bit 2 is set if there are any ballots specified
     *   bit 3 is set if there are any non-standard TxnId.Kind present
     *   bit 4 is set if there are any queries with override flags
     *   bits 6-7 number of header bytes to read for each command
     *   bits 8-9: level 0 extra hlc bytes to read
     *   bits 10-11: level 1 extra hlc bytes to read (+ 1 + level 0)
     *   bits 12-13: level 2 extra hlc bytes to read (+ 1 + level 1)
     *   bits 14-15: level 3 extra hlc bytes to read (+ 1 + level 2)
     *
     * In order, for each command, we consume:
     * 3 bits for the InternalStatus of the command
     * 1 optional bit: if any command has override flags; 2 bits more to read if this bit is set
     * 1 optional bit: if the status encodes an executeAt, indicating if the executeAt is not the TxnId
     * 1 optional bit: if the status encodes any dependencies and there are non-zero missing ids, indicating if there are any missing for this command
     * 2 or 3 bits for the kind of the TxnId
     * 1 bit encoding if the epoch has changed
     * 2 optional bits: if the prior bit is set, indicating how many bits should be read for the epoch increment: 0=none (increment by 1); 1=4, 2=8, 3=32
     * 4 option bits: if prior bits=01, epoch delta
     * N node id bits (where 2^N unique node ids in the CFK)
     * 2 bits indicating how many more payload bytes should be read, with mapping written in header
     * all remaining bits are interpreted as a delta from the prior HLC
     *
     * if txnId kind flag is 3, read an additional 2 bytes for TxnId flag
     * if epoch increment flag is 2 or 3, read additional 1 or 4 bytes for epoch delta
     * if executeAt is expected, read vint32 for epoch, vint32 for delta from txnId hlc, and ceil(N/8) bytes for node id
     *
     * After writing all transactions, we then write out the missing txnid collections. This is written at the end
     * so that on deserialization we have already read all of the TxnId. This also permits more efficient serialization,
     * as we can encode a single bit stream with the optimal number of bits.
     * TODO (desired): we could prefix this collection with the subset of TxnId that are actually missing from any other
     *   deps, so as to shrink this collection much further.
     */
    // TODO (expected): offer filtering option that does not need to reconstruct objects/info, reusing prior encoding decisions
    // TODO (expected): accept new redundantBefore on load to avoid deserializing stale data
    // TODO (desired): determine timestamp resolution as a factor of 10
    public static ByteBuffer toBytesWithoutKey(CommandsForKey cfk)
    {
        Invariants.checkArgument(!cfk.isLoadingPruned());

        int commandCount = cfk.size();
        if (commandCount == 0)
            return ByteBuffer.allocate(1);

        int[] nodeIds = cachedInts().getInts(Math.min(64, Math.max(4, commandCount)));
        try
        {
            // first compute the unique Node Ids and some basic characteristics of the data, such as
            // whether we have any missing transactions to encode, any executeAt that are not equal to their TxnId
            // and whether there are any non-standard flag bits to encode
            boolean hasNonStandardFlags = false;
            int nodeIdCount, missingIdCount = 0, executeAtCount = 0, ballotCount = 0, overrideCount = 0;
            int bitsPerExecuteAtEpoch = 0, bitsPerExecuteAtFlags = 0, bitsPerExecuteAtHlc = 1; // to permit us to use full 64 bits and encode in 5 bits we force at least one hlc bit
            {
                nodeIds[0] = cfk.redundantBefore().node.id;
                nodeIdCount = 1;
                for (int i = 0 ; i < commandCount ; ++i)
                {
                    if (nodeIdCount + 3 >= nodeIds.length)
                    {
                        nodeIdCount = compact(nodeIds, nodeIdCount);
                        if (nodeIdCount > nodeIds.length/2 || nodeIdCount + 2 >= nodeIds.length)
                            nodeIds = cachedInts().resize(nodeIds, nodeIds.length, nodeIds.length * 2);
                    }

                    TxnInfo txn = cfk.get(i);
                    overrideCount += txn.statusOverrides() > 0 ? 1 : 0;
                    hasNonStandardFlags |= hasNonStandardFlags(txn);
                    nodeIds[nodeIdCount++] = txn.node.id;

                    if (txn.executeAt != txn)
                    {
                        Invariants.checkState(txn.status().hasExecuteAtOrDeps);
                        nodeIds[nodeIdCount++] = txn.executeAt.node.id;
                        bitsPerExecuteAtEpoch = Math.max(bitsPerExecuteAtEpoch, numberOfBitsToRepresent(txn.executeAt.epoch() - txn.epoch()));
                        bitsPerExecuteAtHlc = Math.max(bitsPerExecuteAtHlc, numberOfBitsToRepresent(txn.executeAt.hlc() - txn.hlc()));
                        bitsPerExecuteAtFlags = Math.max(bitsPerExecuteAtFlags, numberOfBitsToRepresent(txn.executeAt.flags()));
                        executeAtCount += 1;
                    }

                    if (txn.getClass() == TxnInfoExtra.class)
                    {
                        TxnInfoExtra extra = (TxnInfoExtra) txn;
                        missingIdCount += extra.missing.length;
                        if (extra.ballot != Ballot.ZERO)
                        {
                            Invariants.checkArgument(txn.status().hasBallot);
                            nodeIds[nodeIdCount++] = extra.ballot.node.id;
                            ballotCount += 1;
                        }
                    }
                }
                nodeIdCount = compact(nodeIds, nodeIdCount);
                Invariants.checkState(nodeIdCount > 0);
            }

            // We can now use this information to calculate the fixed header size, compute the amount
            // of additional space we'll need to store the TxnId and its basic info
            int bitsPerNodeId = numberOfBitsToRepresent(nodeIdCount);
            int minHeaderBits = 8 + bitsPerNodeId + (hasNonStandardFlags ? 1 : 0) + (overrideCount > 0 ? 1 : 0);
            int infoHeaderBits = (executeAtCount > 0 ? 1 : 0) + (missingIdCount > 0 ? 1 : 0);
            int ballotHeaderBits = (ballotCount > 0 ? 1 : 0);
            int maxHeaderBits = minHeaderBits;
            int totalBytes = 0;

            int prunedBeforeIndex = cfk.prunedBefore().equals(TxnId.NONE) ? -1 : cfk.indexOf(cfk.prunedBefore());

            long prevEpoch = cfk.redundantBefore().epoch();
            long prevHlc = cfk.redundantBefore().hlc();
            int[] bytesHistogram = cachedInts().getInts(12);
            Arrays.fill(bytesHistogram, 0);
            for (int i = 0 ; i < commandCount ; ++i)
            {
                int headerBits = minHeaderBits;
                int payloadBits = 0;

                TxnId txnId = cfk.txnId(i);
                {
                    long epoch = txnId.epoch();
                    Invariants.checkState(epoch >= prevEpoch);
                    long epochDelta = epoch - prevEpoch;
                    long hlc = txnId.hlc();
                    long hlcDelta = hlc - prevHlc;

                    if (epochDelta > 0)
                    {
                        if (hlcDelta < 0)
                            hlcDelta = -1 - hlcDelta;

                        headerBits += 3;
                        if (epochDelta > 1)
                        {
                            if (epochDelta <= 0xf) headerBits += 4;
                            else if (epochDelta <= 0xff) totalBytes += 1;
                            else { totalBytes += 4; Invariants.checkState(epochDelta <= 0xffffffffL); }
                        }
                    }

                    payloadBits += numberOfBitsToRepresent(hlcDelta);
                    prevEpoch = epoch;
                    prevHlc = hlc;
                }

                if (txnIdFlagsBits(txnId, hasNonStandardFlags) == RAW_BITS)
                    totalBytes += 2;

                TxnInfo info = cfk.get(i);
                if (info.status().hasExecuteAtOrDeps)
                    headerBits += infoHeaderBits;
                if (info.status().hasBallot)
                    headerBits += ballotHeaderBits;
                if (info.statusOverrides() != 0)
                    headerBits += 2;
                maxHeaderBits = Math.max(headerBits, maxHeaderBits);
                int basicBytes = (headerBits + payloadBits + 7)/8;
                bytesHistogram[basicBytes]++;
            }

            int minBasicBytes = -1, maxBasicBytes = 0;
            for (int i = 0 ; i < bytesHistogram.length ; ++i)
            {
                if (bytesHistogram[i] == 0) continue;
                if (minBasicBytes == -1) minBasicBytes = i;
                maxBasicBytes = i;
            }
            for (int i = minBasicBytes + 1 ; i <= maxBasicBytes ; ++i)
                bytesHistogram[i] += bytesHistogram[i-1];

            int flags = (missingIdCount > 0 ? HAS_MISSING_DEPS_HEADER_BIT : 0)
                        | (executeAtCount > 0 ? HAS_EXECUTE_AT_HEADER_BIT : 0)
                        | (ballotCount > 0 ? HAS_BALLOT_HEADER_BIT : 0)
                        | (hasNonStandardFlags ? HAS_NON_STANDARD_FLAGS : 0)
                        | (overrideCount > 0 ? HAS_STATUS_OVERRIDES : 0);

            int headerBytes = (maxHeaderBits+7)/8;
            flags |= Invariants.checkArgument(headerBytes - 1, headerBytes <= 4) << 6;

            int hlcBytesLookup;
            {   // 2bits per size, first value may be zero and remainder may be increments of 1-4;
                // only need to be able to encode a distribution of approx. 8 bytes at most, so
                // pick lowest number we need first, then next lowest as 25th %ile while ensuring value of 1-4;
                // then pick highest number we need, ensuring at least 2 greater than second (leaving room for third)
                // then pick third number as 75th %ile, but at least 1 less than highest, and one more than second
                // finally, ensure third then second are distributed so that there is no more than a gap of 4 between them and the next
                int l0 = Math.max(0, Math.min(3, minBasicBytes - headerBytes));
                int l1 = Arrays.binarySearch(bytesHistogram, minBasicBytes, maxBasicBytes, commandCount/4);
                l1 = Math.max(l0+1, Math.min(l0+4, (l1 < 0 ? -1 - l1 : l1) - headerBytes));
                int l3 = Math.max(l1+2, maxBasicBytes - headerBytes);
                int l2 = Arrays.binarySearch(bytesHistogram, minBasicBytes, maxBasicBytes,(3*commandCount)/4);
                l2 = Math.max(l1+1, Math.min(l3-1, (l2 < 0 ? -1 -l2 : l2) - headerBytes));
                while (l3-l2 > 4) ++l2;
                while (l2-l1 > 4) ++l1;
                hlcBytesLookup = setHlcBytes(l0, l1, l2, l3);
                flags |= (l0 | ((l1-(1+l0))<<2) | ((l2-(1+l1))<<4) | ((l3-(1+l2))<<6)) << 8;
            }
            int hlcFlagLookup = hlcBytesLookupToHlcFlagLookup(hlcBytesLookup);

            totalBytes += bytesHistogram[minBasicBytes] * (headerBytes + getHlcBytes(hlcBytesLookup, getHlcFlag(hlcFlagLookup, minBasicBytes - headerBytes)));
            for (int i = minBasicBytes + 1 ; i <= maxBasicBytes ; ++i)
                totalBytes += (bytesHistogram[i] - bytesHistogram[i-1]) * (headerBytes + getHlcBytes(hlcBytesLookup, getHlcFlag(hlcFlagLookup, i - headerBytes)));
            totalBytes += TypeSizes.sizeofUnsignedVInt(commandCount);
            totalBytes += TypeSizes.sizeofUnsignedVInt(nodeIdCount);
            totalBytes += TypeSizes.sizeofUnsignedVInt(nodeIds[0]);
            for (int i = 1 ; i < nodeIdCount ; ++i)
                totalBytes += TypeSizes.sizeofUnsignedVInt(nodeIds[i] - nodeIds[i-1]);
            totalBytes += 2;

            cachedInts().forceDiscard(bytesHistogram);

            prevEpoch = cfk.redundantBefore().epoch();
            prevHlc = cfk.redundantBefore().hlc();
            {
                RedundantBefore.Entry boundsInfo = cfk.boundsInfo();
                long start = boundsInfo.startOwnershipEpoch;
                long end = boundsInfo.endOwnershipEpoch;
                totalBytes += VIntCoding.computeUnsignedVIntSize(start);
                totalBytes += VIntCoding.computeUnsignedVIntSize(end == Long.MAX_VALUE ? 0 : (1 + end - start));
                totalBytes += VIntCoding.computeVIntSize(prevEpoch - start);
            }
            totalBytes += TypeSizes.sizeofUnsignedVInt(prevHlc);
            totalBytes += TypeSizes.sizeofUnsignedVInt(cfk.redundantBefore().flags());
            totalBytes += TypeSizes.sizeofUnsignedVInt(Arrays.binarySearch(nodeIds, 0, nodeIdCount, cfk.redundantBefore().node.id));
            totalBytes += TypeSizes.sizeofUnsignedVInt(prunedBeforeIndex + 1);

            int bitsPerBallotEpoch = 0, bitsPerBallotHlc = 1, bitsPerBallotFlags = 0;
            if ((missingIdCount | executeAtCount | ballotCount) > 0)
            {
                if (ballotCount > 0)
                {
                    Ballot prevBallot = null;
                    for (int i = 0 ; i < commandCount ; ++i)
                    {
                        TxnInfo txn = cfk.get(i);
                        if (txn.getClass() != TxnInfoExtra.class) continue;
                        if (!txn.status().hasBallot) continue;
                        TxnInfoExtra extra = (TxnInfoExtra) txn;
                        if (extra.ballot == Ballot.ZERO) continue;
                        if (prevBallot != null)
                        {
                            bitsPerBallotEpoch = Math.max(bitsPerBallotEpoch, numberOfBitsToRepresent(encodeZigZag64(extra.ballot.epoch() - prevBallot.epoch())));
                            bitsPerBallotHlc = Math.max(bitsPerBallotHlc, numberOfBitsToRepresent(encodeZigZag64(extra.ballot.hlc() - prevBallot.hlc())));
                            bitsPerBallotFlags = Math.max(bitsPerBallotFlags, numberOfBitsToRepresent(extra.ballot.flags()));
                        }
                        prevBallot = extra.ballot;
                    }
                    totalBytes += 2; // encode bit widths
                }

                if (executeAtCount > 0)
                    totalBytes += 2; // encode bit widths

                // account for encoding missing id stream
                int missingIdBits = 1 + numberOfBitsToRepresent(commandCount);
                int executeAtBits = bitsPerNodeId
                                    + bitsPerExecuteAtEpoch
                                    + bitsPerExecuteAtHlc
                                    + bitsPerExecuteAtFlags;
                int ballotBits = bitsPerNodeId
                                 + bitsPerBallotEpoch
                                 + bitsPerBallotHlc
                                 + bitsPerBallotFlags;
                totalBytes += (missingIdBits * missingIdCount
                               + executeAtBits * executeAtCount
                               + (ballotCount > 0 ? ballotBits * (ballotCount - 1) + bitsPerNodeId + 128 : 0)
                               + 7)/8;
            }

            // count unmanaged bytes
            int unmanagedPendingCommitCount = 0;
            for (int i = 0 ; i < cfk.unmanagedCount() ; ++i)
            {
                Unmanaged unmanaged = cfk.getUnmanaged(i);
                if (unmanaged.pending == Unmanaged.Pending.COMMIT)
                    ++unmanagedPendingCommitCount;
                totalBytes += CommandSerializers.txnId.serializedSize();
                // TODO (desired): this could be more efficient, e.g. referencing one of the TxnInfo indexes for timestamp
                totalBytes += CommandSerializers.timestamp.serializedSize();
            }
            totalBytes += TypeSizes.sizeofUnsignedVInt(unmanagedPendingCommitCount);
            totalBytes += TypeSizes.sizeofUnsignedVInt(cfk.unmanagedCount() - unmanagedPendingCommitCount);

            ByteBuffer out = ByteBuffer.allocate(totalBytes);
            VIntCoding.writeUnsignedVInt32(commandCount, out);
            VIntCoding.writeUnsignedVInt32(nodeIdCount, out);
            VIntCoding.writeUnsignedVInt32(nodeIds[0], out);
            for (int i = 1 ; i < nodeIdCount ; ++i) // TODO (desired): can encode more efficiently as a stream of N bit integers
                VIntCoding.writeUnsignedVInt32(nodeIds[i] - nodeIds[i-1], out);
            out.putShort((short)flags);


            {
                RedundantBefore.Entry boundsInfo = cfk.boundsInfo();
                long start = boundsInfo.startOwnershipEpoch;
                long end = boundsInfo.endOwnershipEpoch;
                VIntCoding.writeUnsignedVInt(start, out);
                VIntCoding.writeUnsignedVInt(end == Long.MAX_VALUE ? 0 : (1 + end - start), out);
                VIntCoding.writeVInt(prevEpoch - start, out);
            }
            VIntCoding.writeUnsignedVInt(prevHlc, out);
            VIntCoding.writeUnsignedVInt32(cfk.redundantBefore().flags(), out);
            VIntCoding.writeUnsignedVInt32(Arrays.binarySearch(nodeIds, 0, nodeIdCount, cfk.redundantBefore().node.id), out);
            VIntCoding.writeUnsignedVInt32(prunedBeforeIndex + 1, out);

            int executeAtMask = executeAtCount > 0 ? 1 : 0;
            int missingDepsMask = missingIdCount > 0 ? 1 : 0;
            int ballotMask = ballotCount > 0 ? 1 : 0;
            int noOverrideIncrement = overrideCount > 0 ? 1 : 0;
            int flagsIncrement = hasNonStandardFlags ? 3 : 2;
            // TODO (desired): check this loop compiles correctly to only branch on epoch case, for binarySearch and flushing
            for (int i = 0 ; i < commandCount ; ++i)
            {
                TxnInfo txn = cfk.get(i);
                InternalStatus status = txn.status();

                long bits = status.ordinal();
                int bitIndex = 3;

                int statusHasInfo = status.hasExecuteAtOrDeps ? 1 : 0;
                int statusHasBallot = status.hasBallot ? 1 : 0;
                long hasExecuteAt = txn.executeAt != txn ? 1 : 0;
                bits |= hasExecuteAt << bitIndex;
                bitIndex += statusHasInfo & executeAtMask;

                long hasMissingIds = txn.getClass() == TxnInfoExtra.class && ((TxnInfoExtra)txn).missing != NO_TXNIDS ? 1 : 0;
                bits |= hasMissingIds << bitIndex;
                bitIndex += statusHasInfo & missingDepsMask;

                long hasBallot = txn.getClass() == TxnInfoExtra.class && ((TxnInfoExtra)txn).ballot != Ballot.ZERO ? 1 : 0;
                bits |= hasBallot << bitIndex;
                bitIndex += statusHasBallot & ballotMask;

                long statusOverrides = (long) txn.statusOverrides() << 1;
                statusOverrides |= statusOverrides != 0 ? 1 : 0;
                bits |= statusOverrides << bitIndex;
                bitIndex += statusOverrides != 0 ? 3 : noOverrideIncrement;

                long flagBits = txnIdFlagsBits(txn, hasNonStandardFlags);
                boolean writeFullFlags = flagBits == RAW_BITS;
                bits |= flagBits << bitIndex;
                bitIndex += flagsIncrement;

                long hlcBits;
                int extraEpochDeltaBytes = 0;
                {
                    long epoch = txn.epoch();
                    long delta = epoch - prevEpoch;
                    long hlc = txn.hlc();
                    hlcBits = hlc - prevHlc;
                    if (delta == 0)
                    {
                        bitIndex++;
                    }
                    else
                    {
                        bits |= 1L << bitIndex++;
                        if (hlcBits < 0)
                        {
                            hlcBits = -1 - hlcBits;
                            bits |= 1L << bitIndex;
                        }
                        bitIndex++;
                        if (delta > 1)
                        {
                            if (delta <= 0xf)
                            {
                                bits |= 1L << bitIndex;
                                bits |= delta << (bitIndex + 2);
                                bitIndex += 4;
                            }
                            else
                            {
                                bits |= (delta <= 0xff ? 2L : 3L) << bitIndex;
                                extraEpochDeltaBytes = Ints.checkedCast(delta);
                            }
                        }
                        bitIndex += 2;
                    }
                    prevEpoch = epoch;
                    prevHlc = hlc;
                }

                bits |= ((long)Arrays.binarySearch(nodeIds, 0, nodeIdCount, txn.node.id)) << bitIndex;
                bitIndex += bitsPerNodeId;

                bits |= hlcBits << (bitIndex + 2);
                hlcBits >>>= 8*headerBytes - (bitIndex + 2);
                int hlcFlag = getHlcFlag(hlcFlagLookup, (7 + numberOfBitsToRepresent(hlcBits))/8);
                bits |= ((long)hlcFlag) << bitIndex;

                writeLeastSignificantBytes(bits, headerBytes, out);
                writeLeastSignificantBytes(hlcBits, getHlcBytes(hlcBytesLookup, hlcFlag), out);

                if (writeFullFlags)
                    out.putShort((short)txn.flags());

                if (extraEpochDeltaBytes > 0)
                {
                    if (extraEpochDeltaBytes <= 0xff) out.put((byte)extraEpochDeltaBytes);
                    else out.putInt(extraEpochDeltaBytes);
                }
            }

            VIntCoding.writeUnsignedVInt32(unmanagedPendingCommitCount, out);
            VIntCoding.writeUnsignedVInt32(cfk.unmanagedCount() - unmanagedPendingCommitCount, out);
            Unmanaged.Pending pending = unmanagedPendingCommitCount == 0 ? Unmanaged.Pending.APPLY : Unmanaged.Pending.COMMIT;
            {
                int offset = 0;
                for (int i = 0 ; i < cfk.unmanagedCount() ; ++i)
                {
                    Unmanaged unmanaged = cfk.getUnmanaged(i);
                    Invariants.checkState(unmanaged.pending == pending);

                    offset += CommandSerializers.txnId.serialize(unmanaged.txnId, out, ByteBufferAccessor.instance, offset);
                    offset += CommandSerializers.timestamp.serialize(unmanaged.waitingUntil, out, ByteBufferAccessor.instance, offset);
                    if (--unmanagedPendingCommitCount == 0) pending = Unmanaged.Pending.APPLY;
                }
                out.position(out.position() + offset);
            }

            if ((executeAtCount | missingIdCount | ballotCount) > 0)
            {
                int bitsPerCommandId =  numberOfBitsToRepresent(commandCount);
                int bitsPerMissingId = 1 + bitsPerCommandId;
                int bitsPerExecuteAt = bitsPerExecuteAtEpoch + bitsPerExecuteAtHlc + bitsPerExecuteAtFlags + bitsPerNodeId;
                int bitsPerBallot = bitsPerBallotEpoch + bitsPerBallotHlc + bitsPerBallotFlags + bitsPerNodeId;
                Invariants.checkState(bitsPerExecuteAtEpoch < 64);
                Invariants.checkState(bitsPerExecuteAtHlc <= 64);
                Invariants.checkState(bitsPerExecuteAtFlags <= 16);
                if (executeAtMask > 0) // we encode both 15 and 16 bits for flag length as 15 to fit in a short
                    out.putShort((short) ((bitsPerExecuteAtEpoch << 10) | ((bitsPerExecuteAtHlc-1) << 4) | (Math.min(15, bitsPerExecuteAtFlags))));
                if (ballotMask > 0) // we encode both 15 and 16 bits for flag length as 15 to fit in a short
                    out.putShort((short) ((bitsPerBallotEpoch << 10) | ((bitsPerBallotHlc-1) << 4) | (Math.min(15, bitsPerBallotFlags))));
                long buffer = 0L;
                int bufferCount = 0;

                Ballot prevBallot = null;
                for (int i = 0 ; i < commandCount ; ++i)
                {
                    TxnInfo txn = cfk.get(i);
                    if (txn.executeAt != txn)
                    {
                        Timestamp executeAt = txn.executeAt;
                        int nodeIdx = Arrays.binarySearch(nodeIds, 0, nodeIdCount, executeAt.node.id);
                        if (bitsPerExecuteAt <= 64)
                        {
                            Invariants.checkState(executeAt.epoch() >= txn.epoch());
                            long executeAtBits = executeAt.epoch() - txn.epoch();
                            int offset = bitsPerExecuteAtEpoch;
                            executeAtBits |= (executeAt.hlc() - txn.hlc()) << offset ;
                            offset += bitsPerExecuteAtHlc;
                            executeAtBits |= ((long)executeAt.flags()) << offset;
                            offset += bitsPerExecuteAtFlags;
                            executeAtBits |= ((long)nodeIdx) << offset;
                            buffer = flushBits(buffer, bufferCount, executeAtBits, bitsPerExecuteAt, out);
                            bufferCount = (bufferCount + bitsPerExecuteAt) & 63;
                        }
                        else
                        {
                            buffer = flushBits(buffer, bufferCount, executeAt.epoch() - txn.epoch(), bitsPerExecuteAtEpoch, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtEpoch) & 63;
                            buffer = flushBits(buffer, bufferCount, executeAt.hlc() - txn.hlc(), bitsPerExecuteAtHlc, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtHlc) & 63;
                            buffer = flushBits(buffer, bufferCount, executeAt.flags(), bitsPerExecuteAtFlags, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtFlags) & 63;
                            buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                            bufferCount = (bufferCount + bitsPerNodeId) & 63;
                        }
                    }

                    if (txn.getClass() == TxnInfoExtra.class)
                    {
                        TxnInfoExtra extra = (TxnInfoExtra) txn;

                        TxnId[] missing = extra.missing;
                        if (missing.length > 0)
                        {
                            int j = 0;
                            while (j < missing.length - 1)
                            {
                                int missingId = cfk.indexOf(missing[j++]);
                                buffer = flushBits(buffer, bufferCount, missingId, bitsPerMissingId, out);
                                bufferCount = (bufferCount + bitsPerMissingId) & 63;
                            }
                            int missingId = cfk.indexOf(missing[missing.length - 1]);
                            missingId |= 1L << bitsPerCommandId;
                            buffer = flushBits(buffer, bufferCount, missingId, bitsPerMissingId, out);
                            bufferCount = (bufferCount + bitsPerMissingId) & 63;
                        }

                        Ballot ballot = extra.ballot;
                        if (ballot != Ballot.ZERO)
                        {
                            int nodeIdx = Arrays.binarySearch(nodeIds, 0, nodeIdCount, ballot.node.id);
                            if (prevBallot == null)
                            {
                                buffer = flushBits(buffer, bufferCount, ballot.msb, 64, out);
                                buffer = flushBits(buffer, bufferCount, ballot.lsb, 64, out);
                                buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                                bufferCount = (bufferCount + bitsPerNodeId) & 63;
                            }
                            else if (bitsPerBallot <= 64)
                            {
                                long ballotBits = encodeZigZag64(ballot.epoch() - prevBallot.epoch());
                                int offset = bitsPerBallotEpoch;
                                ballotBits |= encodeZigZag64(ballot.hlc() - prevBallot.hlc()) << offset ;
                                offset += bitsPerBallotHlc;
                                ballotBits |= ((long)ballot.flags()) << offset;
                                offset += bitsPerBallotFlags;
                                ballotBits |= ((long)nodeIdx) << offset;
                                buffer = flushBits(buffer, bufferCount, ballotBits, bitsPerBallot, out);
                                bufferCount = (bufferCount + bitsPerBallot) & 63;
                            }
                            else
                            {
                                buffer = flushBits(buffer, bufferCount, encodeZigZag64(ballot.epoch() - prevBallot.epoch()), bitsPerBallotEpoch, out);
                                bufferCount = (bufferCount + bitsPerBallotEpoch) & 63;
                                buffer = flushBits(buffer, bufferCount, encodeZigZag64(ballot.hlc() - prevBallot.hlc()), bitsPerBallotHlc, out);
                                bufferCount = (bufferCount + bitsPerBallotHlc) & 63;
                                buffer = flushBits(buffer, bufferCount, ballot.flags(), bitsPerBallotFlags, out);
                                bufferCount = (bufferCount + bitsPerBallotFlags) & 63;
                                buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                                bufferCount = (bufferCount + bitsPerNodeId) & 63;
                            }
                            prevBallot = ballot;
                        }
                    }
                }

                writeMostSignificantBytes(buffer, (bufferCount + 7)/8, out);
            }

            Invariants.checkState(!out.hasRemaining());
            out.flip();
            return out;
        }
        finally
        {
            cachedInts().forceDiscard(nodeIds);
        }
    }

    private static long flushBits(long buffer, int bufferCount, long add, int addCount, ByteBuffer out)
    {
        Invariants.checkArgument(addCount == 64 || 0 == (add & (-1L << addCount)));
        int total = bufferCount + addCount;
        if (total < 64)
        {
            return buffer | (add << 64 - total);
        }
        else
        {
            buffer |= add >>> total - 64;
            out.putLong(buffer);
            return total == 64 ? 0 : (add << (128 - total));
        }
    }

    public static CommandsForKey fromBytes(RoutingKey key, ByteBuffer in)
    {
        if (!in.hasRemaining())
            return null;

        in = in.duplicate();
        int commandCount = VIntCoding.readUnsignedVInt32(in);
        if (commandCount == 0)
            return new CommandsForKey(key);

        TxnId[] txnIds = cachedTxnIds().get(commandCount);
        int[] decodeFlags = cachedInts().getInts(commandCount);
        TxnInfo[] txns = new TxnInfo[commandCount];
        int nodeIdCount = VIntCoding.readUnsignedVInt32(in);
        int bitsPerNodeId = numberOfBitsToRepresent(nodeIdCount);
        long nodeIdMask = (1L << bitsPerNodeId) - 1;
        Node.Id[] nodeIds = new Node.Id[nodeIdCount]; // TODO (expected): use a shared reusable scratch buffer
        {
            int prev = VIntCoding.readUnsignedVInt32(in);
            nodeIds[0] = new Node.Id(prev);
            for (int i = 1 ; i < nodeIdCount ; ++i)
                nodeIds[i] = new Node.Id(prev += VIntCoding.readUnsignedVInt32(in));
        }

        int missingDepsMasks, executeAtMasks, ballotMasks, txnIdFlagsMask, overrideMask;
        int headerByteCount, hlcBytesLookup;
        {
            int flags = in.getShort();
            missingDepsMasks = 0 != (flags & HAS_MISSING_DEPS_HEADER_BIT) ? 1 : 0;
            executeAtMasks = 0 != (flags & HAS_EXECUTE_AT_HEADER_BIT) ? 1 : 0;
            ballotMasks = 0 != (flags & HAS_BALLOT_HEADER_BIT) ? 1 : 0;
            overrideMask = 0 != (flags & HAS_STATUS_OVERRIDES) ? 1 : 0;
            txnIdFlagsMask = 0 != (flags & HAS_NON_STANDARD_FLAGS) ? 7 : 3;
            headerByteCount = 1 + ((flags >>> 6) & 0x3);
            hlcBytesLookup = setHlcByteDeltas((flags >>> 8) & 0x3, (flags >>> 10) & 0x3, (flags >>> 12) & 0x3, (flags >>> 14) & 0x3);
        }

        long minEpoch = VIntCoding.readUnsignedVInt(in);
        long maxEpoch; {
            long offset = VIntCoding.readUnsignedVInt(in);
            maxEpoch = offset == 0 ? Long.MAX_VALUE : minEpoch + offset - 1;
        }
        RedundantBefore.Entry boundsInfo = NO_BOUNDS_INFO.withEpochs(minEpoch, maxEpoch);
        long prevEpoch = minEpoch + VIntCoding.readVInt(in);
        long prevHlc = VIntCoding.readUnsignedVInt(in);
        {
            int flags = VIntCoding.readUnsignedVInt32(in);
            Node.Id node = nodeIds[VIntCoding.readUnsignedVInt32(in)];
            boundsInfo = boundsInfo.withGcBeforeBeforeAtLeast(TxnId.fromValues(prevEpoch, prevHlc, flags, node));
        }
        int prunedBeforeIndex = VIntCoding.readUnsignedVInt32(in) - 1;

        for (int i = 0 ; i < commandCount ; ++i)
        {
            long header = readLeastSignificantBytes(headerByteCount, in);
            header |= 1L << (8 * headerByteCount); // marker so we know where to shift-left most-significant bytes to
            int commandDecodeFlags = (int)(header & 0x7);
            InternalStatus status = InternalStatus.get(commandDecodeFlags);
            header >>>= 3;
            commandDecodeFlags <<= 6;

            {
                int infoMask = status.hasExecuteAtOrDeps ? 1 : 0;
                int executeAtMask = infoMask & executeAtMasks;
                int missingDepsMask = infoMask & missingDepsMasks;
                commandDecodeFlags |= ((int)header & executeAtMask) << 1;
                header >>>= executeAtMask;
                commandDecodeFlags |= ((int)header & missingDepsMask);
                header >>>= missingDepsMask;
                int ballotMask = status.hasBallot ? ballotMasks : 0;
                commandDecodeFlags |= ((int)header & ballotMask) << 2;
                header >>>= ballotMask;
                commandDecodeFlags |= (header & 0x7) << 3;
                header >>= (header & overrideMask) == 0 ? overrideMask : 3;
                decodeFlags[i] = commandDecodeFlags;
            }

            Txn.Kind kind; Domain domain; {
                int flags = (int)header & txnIdFlagsMask;
                kind = kindLookup(flags);
                domain = domainLookup(flags);
            }
            header >>>= Integer.bitCount(txnIdFlagsMask);

            boolean hlcIsNegative = false;
            long epoch = prevEpoch;
            int readEpochBytes = 0;
            {
                boolean hasEpochDelta = (header & 1) == 1;
                header >>>= 1;
                if (hasEpochDelta)
                {
                    hlcIsNegative = (header & 1) == 1;
                    header >>>= 1;

                    int epochFlag = ((int)header & 0x3);
                    header >>>= 2;
                    switch (epochFlag)
                    {
                        default: throw new AssertionError("Unexpected value not 0-3");
                        case 0: ++epoch; break;
                        case 1: epoch += (header & 0xf); header >>>= 4; break;
                        case 2: readEpochBytes = 1; break;
                        case 3: readEpochBytes = 4; break;
                    }
                }
            }

            Node.Id node = nodeIds[(int)(header & nodeIdMask)];
            header >>>= bitsPerNodeId;

            int readHlcBytes = getHlcBytes(hlcBytesLookup, (int)(header & 0x3));
            header >>>= 2;

            long hlc = header;
            {
                long highestBit = Long.highestOneBit(hlc);
                hlc ^= highestBit;
                int hlcShift = Long.numberOfTrailingZeros(highestBit);
                hlc |= readLeastSignificantBytes(readHlcBytes, in) << hlcShift;
            }
            if (hlcIsNegative)
                hlc = -1-hlc;
            hlc += prevHlc;

            int flags = kind != null ? 0 : in.getShort();
            if (readEpochBytes > 0)
                epoch += readEpochBytes == 1 ? (in.get() & 0xff) : in.getInt();

            txnIds[i] = kind != null ? new TxnId(epoch, hlc, kind, domain, node)
                                     : TxnId.fromValues(epoch, hlc, flags, node);

            prevEpoch = epoch;
            prevHlc = hlc;
        }

        int unmanagedPendingCommitCount = VIntCoding.readUnsignedVInt32(in);
        int unmanagedCount = unmanagedPendingCommitCount + VIntCoding.readUnsignedVInt32(in);
        Unmanaged[] unmanageds;
        if (unmanagedCount == 0)
        {
            unmanageds = NO_PENDING_UNMANAGED;
        }
        else
        {
            unmanageds = new Unmanaged[unmanagedCount];
            Unmanaged.Pending pending = unmanagedPendingCommitCount == 0 ? Unmanaged.Pending.APPLY : Unmanaged.Pending.COMMIT;
            int offset = 0;
            for (int i = 0 ; i < unmanagedCount ; ++i)
            {
                TxnId txnId = CommandSerializers.txnId.deserialize(in, ByteBufferAccessor.instance, offset);
                offset += CommandSerializers.txnId.serializedSize();
                Timestamp waitingUntil = CommandSerializers.timestamp.deserialize(in, ByteBufferAccessor.instance, offset);
                offset += CommandSerializers.timestamp.serializedSize();
                unmanageds[i] = new Unmanaged(pending, txnId, waitingUntil);
                if (--unmanagedPendingCommitCount == 0) pending = Unmanaged.Pending.APPLY;
            }
            in.position(in.position() + offset);
        }

        if ((executeAtMasks | missingDepsMasks | ballotMasks) > 0)
        {
            TxnId[] missingIdBuffer = cachedTxnIds().get(8);
            int missingIdCount = 0, maxIdBufferCount = 0;
            int bitsPerTxnId = numberOfBitsToRepresent(commandCount);
            int txnIdMask = (1 << bitsPerTxnId) - 1;
            int bitsPerMissingId = bitsPerTxnId + 1;

            int decodeExecuteAtBits = executeAtMasks > 0 ? in.getShort() & 0xffff : 0;
            int bitsPerExecuteAtEpoch = decodeExecuteAtBits >>> 10;
            int bitsPerExecuteAtHlc = 1 + ((decodeExecuteAtBits >>> 4) & 0x3f);
            int bitsPerExecuteAtFlags = decodeExecuteAtBits & 0xf;
            if (bitsPerExecuteAtFlags == 15) bitsPerExecuteAtFlags = 16;
            int bitsPerExecuteAt = bitsPerExecuteAtEpoch + bitsPerExecuteAtHlc + bitsPerExecuteAtFlags + bitsPerNodeId;

            long executeAtEpochMask = bitsPerExecuteAtEpoch == 0 ? 0 : (-1L >>> (64 - bitsPerExecuteAtEpoch));
            long executeAtHlcMask = (-1L >>> (64 - bitsPerExecuteAtHlc));
            long executeAtFlagsMask = bitsPerExecuteAtFlags == 0 ? 0 : (-1L >>> (64 - bitsPerExecuteAtFlags));

            int decodeBallotBits = ballotMasks > 0 ? in.getShort() & 0xffff : 0;
            int bitsPerBallotEpoch = decodeBallotBits >>> 10;
            int bitsPerBallotHlc = 1 + ((decodeBallotBits >>> 4) & 0x3f);
            int bitsPerBallotFlags = decodeBallotBits & 0xf;
            if (bitsPerBallotFlags == 15) bitsPerBallotFlags = 16;
            int bitsPerBallot = bitsPerBallotEpoch + bitsPerBallotHlc + bitsPerBallotFlags + bitsPerNodeId;

            long ballotEpochMask = bitsPerBallotEpoch == 0 ? 0 : (-1L >>> (64 - bitsPerBallotEpoch));
            long ballotHlcMask = (-1L >>> (64 - bitsPerBallotHlc));
            long ballotFlagsMask = bitsPerBallotFlags == 0 ? 0 : (-1L >>> (64 - bitsPerBallotFlags));

            Ballot prevBallot = null;
            final BitReader reader = new BitReader();
            for (int i = 0 ; i < commandCount ; ++i)
            {
                TxnId txnId = txnIds[i];
                int commandDecodeFlags = decodeFlags[i];
                Timestamp executeAt = txnId;
                if ((commandDecodeFlags & HAS_EXECUTE_AT_HEADER_BIT) != 0)
                {
                    long epoch, hlc;
                    int flags;
                    Node.Id id;
                    if (bitsPerExecuteAt <= 64)
                    {
                        long executeAtBits = reader.read(bitsPerExecuteAt, in);
                        epoch = txnId.epoch() + (executeAtBits & executeAtEpochMask);
                        executeAtBits >>>= bitsPerExecuteAtEpoch;
                        hlc = txnId.hlc() + (executeAtBits & executeAtHlcMask);
                        executeAtBits >>>= bitsPerExecuteAtHlc;
                        flags = (int)(executeAtBits & executeAtFlagsMask);
                        executeAtBits >>>= bitsPerExecuteAtFlags;
                        id = nodeIds[(int)(executeAtBits & nodeIdMask)];
                    }
                    else
                    {
                        epoch = txnId.epoch() + reader.read(bitsPerExecuteAtEpoch, in);
                        hlc = txnId.hlc() + reader.read(bitsPerExecuteAtHlc, in);
                        flags = (int) reader.read(bitsPerExecuteAtFlags, in);
                        id = nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                    }
                    executeAt = Timestamp.fromValues(epoch, hlc, flags, id);
                }

                TxnId[] missing = NO_TXNIDS;
                if ((commandDecodeFlags & HAS_MISSING_DEPS_HEADER_BIT) != 0)
                {
                    int prev = -1;
                    while (true)
                    {
                        if (missingIdCount == missingIdBuffer.length)
                            missingIdBuffer = cachedTxnIds().resize(missingIdBuffer, missingIdCount, missingIdCount * 2);

                        int next = (int) reader.read(bitsPerMissingId, in);
                        Invariants.checkState(next > prev);
                        missingIdBuffer[missingIdCount++] = txnIds[next & txnIdMask];
                        if (next >= commandCount)
                            break; // finished this array
                        prev = next;
                    }

                    missing = Arrays.copyOf(missingIdBuffer, missingIdCount);
                    maxIdBufferCount = missingIdCount;
                    missingIdCount = 0;
                }

                Ballot ballot = Ballot.ZERO;
                if ((commandDecodeFlags & HAS_BALLOT_HEADER_BIT) != 0)
                {
                    if (prevBallot == null)
                    {
                        long msb = reader.read(64, in);
                        long lsb = reader.read(64, in);
                        Node.Id id = nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                        ballot = Ballot.fromBits(msb, lsb, id);
                    }
                    else
                    {
                        long epoch, hlc;
                        int flags;
                        Node.Id id;
                        if (bitsPerExecuteAt <= 64)
                        {
                            long ballotBits = reader.read(bitsPerBallot, in);
                            epoch = prevBallot.epoch() + decodeZigZag64(ballotBits & ballotEpochMask);
                            ballotBits >>>= bitsPerBallotEpoch;
                            hlc = prevBallot.hlc() + decodeZigZag64(ballotBits & ballotHlcMask);
                            ballotBits >>>= bitsPerBallotHlc;
                            flags = (int)(ballotBits & ballotFlagsMask);
                            ballotBits >>>= bitsPerBallotFlags;
                            id = nodeIds[(int)(ballotBits & nodeIdMask)];
                        }
                        else
                        {
                            epoch = prevBallot.epoch() + decodeZigZag64(reader.read(bitsPerBallotEpoch, in));
                            hlc = prevBallot.hlc() + decodeZigZag64(reader.read(bitsPerBallotHlc, in));
                            flags = (int) reader.read(bitsPerBallotFlags, in);
                            id = nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                        }
                        ballot = Ballot.fromValues(epoch, hlc, flags, id);
                    }

                    prevBallot = ballot;
                }

                InternalStatus status = InternalStatus.get(commandDecodeFlags >>> 6);
                int statusOverrides = ((commandDecodeFlags >>> 3) & overrideMask) == 0 ? 0 : commandDecodeFlags >>> 4;
                txns[i] = create(boundsInfo, txnId, status, statusOverrides, executeAt, missing, ballot);
            }

            cachedTxnIds().forceDiscard(missingIdBuffer, maxIdBufferCount);
        }
        else
        {
            for (int i = 0 ; i < commandCount ; ++i)
            {
                int commandDecodeFlags = decodeFlags[i];
                InternalStatus status = InternalStatus.get(commandDecodeFlags >>> 6);
                int statusOverrides = ((commandDecodeFlags >>> 3) & overrideMask) == 0 ? 0 : commandDecodeFlags >>> 4;
                txns[i] = create(boundsInfo, txnIds[i], status, statusOverrides, txnIds[i], NO_TXNIDS, Ballot.ZERO);
            }
        }
        cachedTxnIds().forceDiscard(txnIds, commandCount);

        return CommandsForKey.SerializerSupport.create(key, txns, unmanageds, prunedBeforeIndex == -1 ? TxnId.NONE : txns[prunedBeforeIndex], boundsInfo);
    }

    private static TxnInfo create(RedundantBefore.Entry boundsInfo, @Nonnull TxnId txnId, InternalStatus status, int statusOverrides, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
    {
        boolean mayExecute = status.isCommittedToExecute() ? CommandsForKey.executes(boundsInfo, txnId, executeAt)
                                                           : CommandsForKey.mayExecute(boundsInfo, txnId);
        return TxnInfo.create(txnId, status, mayExecute, statusOverrides, executeAt, missing, ballot);
    }

    private static int getHlcBytes(int lookup, int index)
    {
        return (lookup >>> (index * 4)) & 0xf;
    }

    private static int setHlcBytes(int value1, int value2, int value3, int value4)
    {
        return value1 | (value2 << 4) | (value3 << 8) | (value4 << 12);
    }

    private static int setHlcByteDeltas(int value1, int value2, int value3, int value4)
    {
        value2 += 1 + value1;
        value3 += 1 + value2;
        value4 += 1 + value3;
        return setHlcBytes(value1, value2, value3, value4);
    }

    private static int getHlcFlag(int flagsLookup, int bytes)
    {
        return (flagsLookup >>> (bytes * 2)) & 0x3;
    }

    private static int hlcBytesLookupToHlcFlagLookup(int bytesLookup)
    {
        int flagsLookup = 0;
        int flagIndex = 0;
        for (int bytesIndex = 0 ; bytesIndex < 4 ; bytesIndex++)
        {
            int flagLimit = getHlcBytes(bytesLookup, bytesIndex);
            while (flagIndex <= flagLimit)
                flagsLookup |= bytesIndex << (2 * flagIndex++);
        }
        return flagsLookup;
    }

    private static int compact(int[] buffer, int usedSize)
    {
        Arrays.sort(buffer, 0, usedSize);
        int count = 0;
        int j = 0;
        while (j < usedSize)
        {
            int prev;
            buffer[count++] = prev = buffer[j];
            while (++j < usedSize && buffer[j] == prev) {}
        }
        return count;
    }

    private static int numberOfBitsToRepresent(long value)
    {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    private static int numberOfBitsToRepresent(int value)
    {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    static final class BitReader
    {
        private long bitBuffer;
        private int bitCount;

        long read(int readCount, ByteBuffer in)
        {
            if (readCount == 64 && bitCount == 0)
                return in.getLong();

            long result = bitBuffer >>> (64 - readCount);
            int remaining = bitCount - readCount;
            if (remaining >= 0)
            {
                bitBuffer <<= readCount;
                bitCount = remaining;
            }
            else if (in.remaining() >= 8)
            {
                readCount -= bitCount;
                bitBuffer = in.getLong();
                bitCount = 64 - readCount;
                result |= (bitBuffer >>> bitCount);
                bitBuffer <<= readCount;
            }
            else
            {
                readCount -= bitCount;
                while (readCount > 8)
                {
                    long next = in.get() & 0xff;
                    readCount -= 8;
                    result |= next << readCount;
                }
                long next = in.get() & 0xff;
                bitCount = 8 - readCount;
                result |= next >>> bitCount;
                bitBuffer = next << (64 - bitCount);
            }
            return result;
        }
    }

    enum TxnIdFlags
    {
        STANDARD, EXTENDED, RAW;
        static final int RAW_BITS = 0;
    }

    private static boolean hasNonStandardFlags(TxnId txnId)
    {
        if (txnId.flags() > Timestamp.IDENTITY_FLAGS)
            return false;

        int flagBits = txnIdFlagsBits(txnId, true);
        return flagBits > 3;
    }

    private static int txnIdFlagsBits(TxnId txnId, boolean permitNonStandardFlags)
    {
        Txn.Kind kind = txnId.kind();
        Domain domain = txnId.domain();
        if (!permitNonStandardFlags && domain == Domain.Range)
            return 0;

        int offset = domain == Domain.Range ? 3 : 0;
        switch (kind)
        {
            case Read: return offset + 1;
            case Write: return offset + 2;
            case SyncPoint: return offset + 3;
            case ExclusiveSyncPoint:
                if (domain == Domain.Range)
                    return 7;
            default:
                return 0;
        }
    }

    private static Domain domainLookup(int flags)
    {
        return flags <= 4 ? Domain.Key : Domain.Range;
    }

    private static Txn.Kind kindLookup(int flags)
    {
        return TXN_ID_FLAG_BITS_KIND_LOOKUP[flags];
    }

    private static final Txn.Kind[] TXN_ID_FLAG_BITS_KIND_LOOKUP = new Txn.Kind[] { null, Read, Write, SyncPoint, Read, Write, SyncPoint, ExclusiveSyncPoint };
}