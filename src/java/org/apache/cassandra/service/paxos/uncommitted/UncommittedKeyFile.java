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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosKeyState.KEY_COMPARATOR;

/**
 * Contains the keys and corresponding ballots for a given table's uncommitted paxos operations. As updates to the paxos
 * table are flushed to disk, in memory updates are merged with the current file's contents and written to a new file.
 * For a given table there is at most one active uncommitted key file (although there will temporarily be 2 when a new
 * file is being written), so no merging needs to be performed when querying the contents of the file.
 *
 * This is a flat file, with no index or methods of jumping to spots withing a file, other than reading sequentially from
 * the beginning. There shouldn't be too much data in these, and they are only ready occasionally and not in response to
 * client requests, so this should be ok. Just don't do a lot of random reads against them.
 */
class UncommittedKeyFile
{
    private static final int VERSION = 0;
    private final UUID cfId;
    private final File file;
    private final File crcFile;
    final long generation;

    int activeReaders = 0;
    boolean markedDeleted = false;

    UncommittedKeyFile(UUID cfId, File file, File crcFile, long generation)
    {
        this.cfId = cfId;
        this.file = file;
        this.crcFile = crcFile;
        this.generation = generation;
    }

    synchronized void markDeleted()
    {
        markedDeleted = true;
        maybeDelete();
    }

    private void maybeDelete()
    {
        if (markedDeleted && activeReaders == 0)
        {
            FileUtils.deleteWithConfirm(file);
            FileUtils.deleteWithConfirm(crcFile);
        }
    }

    synchronized private void onReaderClose()
    {
        activeReaders--;
        maybeDelete();
    }

    @VisibleForTesting
    File getFile()
    {
        return file;
    }

    private static final Pattern FILE_PATTERN = Pattern.compile("^([a-fA-F0-9]{8}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{4}\\-[a-fA-F0-9]{12})\\-(\\d+)\\.paxos$");

    static Set<UUID> listCfids(File directory)
    {
        Preconditions.checkArgument(directory.isDirectory());
        Set<UUID> cfids = new HashSet<>();
        for (String fname : directory.list())
        {
            Matcher matcher = FILE_PATTERN.matcher(fname);
            if (matcher.matches())
                cfids.add(UUID.fromString(matcher.group(1)));
        }
        return cfids;
    }

    static UncommittedKeyFile cleanupAndLoadMostRecent(File directory, UUID cfId)
    {
        long maxGeneration = Long.MIN_VALUE;
        List<UncommittedKeyFile> files = new ArrayList<>();
        String prefix = cfId.toString();
        Pattern pattern = Pattern.compile(prefix + "-(\\d+)\\.paxos.*");
        for (String fname : directory.list())
        {
            Matcher matcher = pattern.matcher(fname);
            if (!matcher.matches())
                continue;

            File file = new File(directory, fname);
            if (fname.startsWith(prefix) && fname.endsWith(".tmp"))
                FileUtils.deleteWithConfirm(file);

            if (fname.endsWith(".crc"))
                continue;

            File crcFile = new File(directory, crcName(fname));
            if (!crcFile.exists())
                throw new FSReadError(new IOException(String.format("%s does not have a corresponding crc file", file)), crcFile);

            long generation = Long.parseLong(matcher.group(1));
            files.add(new UncommittedKeyFile(cfId, file, crcFile, generation));
            maxGeneration = Math.max(maxGeneration, generation);
        }

        UncommittedKeyFile currentFile = null;
        for (UncommittedKeyFile file : files)
        {
            if (file.generation == maxGeneration)
                currentFile = file;
            else
                file.markDeleted();
        }

        if (currentFile != null)
        {
            // cleanup orphaned crc files
            for (String fname : directory.list())
            {
                if (fname.startsWith(prefix) && fname.endsWith(".crc") && !currentFile.crcFile.getName().equals(fname))
                    FileUtils.deleteWithConfirm(new File(directory, fname));
            }
        }

        return currentFile;
    }

    private static String fileName(UUID cfid, long generation)
    {
        return String.format("%s-%s.paxos", cfid, generation);
    }

    private static String crcName(UUID cfid, long generation)
    {
        return crcName(fileName(cfid, generation));
    }

    private static String crcName(String fname)
    {
        return fname + ".crc";
    }

    static class Writer
    {
        private final File directory;
        private final UUID cfId;
        private final long generation;

        private final File file;
        private final File crcFile;
        private final SequentialWriter writer;
        DecoratedKey lastKey = null;

        Writer(File directory, UUID cfId, long generation) throws IOException
        {
            this.directory = directory;
            this.cfId = cfId;
            this.generation = generation;

            FileUtils.createDirectory(directory);

            this.file = new File(this.directory, fileName(cfId, generation) + ".tmp");
            this.crcFile = new File(this.directory, crcName(cfId, generation) + ".tmp");
            this.writer = new ChecksummedSequentialWriter(file, 64 * 1024, crcFile);
            this.writer.writeInt(VERSION);
        }

        void append(PaxosKeyState state) throws IOException
        {
            Preconditions.checkArgument(!state.committed);
            if (lastKey != null)
                Preconditions.checkArgument(state.key.compareTo(lastKey) > 0);
            lastKey = state.key;
            ByteBufferUtil.writeWithShortLength(state.key.getKey(), writer);
            UUIDSerializer.serializer.serialize(state.ballot, writer, MessagingService.current_version);
        }

        Throwable abort(Throwable accumulate)
        {
            return writer.abort(accumulate);
        }

        UncommittedKeyFile finish()
        {
            writer.finish();
            File finalCrc = new File(directory, crcName(cfId, generation));
            FileUtils.renameWithConfirm(crcFile, finalCrc);
            File finalFile = new File(directory, fileName(cfId, generation));
            FileUtils.renameWithConfirm(file, finalFile);
            return new UncommittedKeyFile(cfId, finalFile, finalCrc, generation);
        }
    }

    private interface PeekingKeyCommitIterator extends CloseableIterator<PaxosKeyState>, PeekingIterator<PaxosKeyState>
    {
        static final PeekingKeyCommitIterator EMPTY = new PeekingKeyCommitIterator()
        {
            public PaxosKeyState peek() { throw new NoSuchElementException(); }
            public void remove() { throw new NoSuchElementException(); }
            public void close() { }
            public boolean hasNext() { return false; }
            public PaxosKeyState next() { throw new NoSuchElementException(); }
        };
    }

    class KeyCommitStateIterator extends AbstractIterator<PaxosKeyState> implements PeekingKeyCommitIterator
    {
        private final Iterator<Range<Token>> rangeIterator;
        private final RandomAccessReader reader;

        private Range<PartitionPosition> currentRange;

        private KeyCommitStateIterator(Collection<Range<Token>> ranges)
        {
            this.rangeIterator = ranges.iterator();
            try
            {
                this.reader = new ChecksummedRandomAccessReader.Builder(file, crcFile).build();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file);
            }
            validateVersion(this.reader);

            Preconditions.checkArgument(rangeIterator.hasNext());
            currentRange = convertRange(rangeIterator.next());
        }

        private Range<PartitionPosition> convertRange(Range<Token> tokenRange)
        {
            return new Range<>(tokenRange.left.maxKeyBound(), tokenRange.right.maxKeyBound());
        }

        private void validateVersion(RandomAccessReader reader)
        {
            try
            {
                int version = reader.readInt();
                Preconditions.checkArgument(version == VERSION, "unsupported file version: %s", version);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file);
            }
        }

        protected PaxosKeyState computeNext()
        {
            try
            {
                nextKey:
                while (!reader.isEOF())
                {
                    DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(reader));
                    UUID ballot = UUIDSerializer.serializer.deserialize(reader, MessagingService.current_version);

                    while (!currentRange.contains(key))
                    {
                        // if this falls before our current target range, just keep going
                        if (currentRange.left.compareTo(key) >= 0)
                            continue nextKey;

                        // otherwise check against subsequent ranges and end iteration if there are none
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = convertRange(rangeIterator.next());
                    }

                    // all commit data on disk is uncommitted
                    return new PaxosKeyState(cfId, key, ballot, false);
                }
                return endOfData();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file);
            }
        }

        public void close()
        {
            onReaderClose();
        }
    }

    private synchronized PeekingKeyCommitIterator iteratorInternal(Collection<Range<Token>> ranges)
    {
        if (markedDeleted)
            return null;
        activeReaders++;
        return new KeyCommitStateIterator(ranges);
    }

    /**
     * Return an iterator of the file contents for the given token ranges. Token ranges
     * must be normalized
     */
    CloseableIterator<PaxosKeyState> iterator(Collection<Range<Token>> ranges)
    {
        Preconditions.checkArgument(Iterables.elementsEqual(Range.normalize(ranges), ranges));
        return iteratorInternal(ranges);
    }

    public static class MergeWriter
    {
        private static final Collection<Range<Token>> FULL_RANGE;
        static
        {
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            FULL_RANGE = Collections.singleton(new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        }

        private final UUID cfid;
        private final File directory;
        private final UncommittedKeyFile current;
        private final PeekingKeyCommitIterator iterator;
        private UncommittedKeyFile.Writer writer = null;

        MergeWriter(UUID cfid, File directory, UncommittedKeyFile current)
        {
            this.cfid = cfid;
            this.directory = directory;
            this.current = current;
            this.iterator = current != null ? current.iteratorInternal(FULL_RANGE) : PeekingKeyCommitIterator.EMPTY;
        }

        private void maybeAppend(PaxosKeyState commit) throws IOException
        {
            if (writer == null)
            {
                long generation = current != null ? current.generation + 1 : 0;
                writer = new UncommittedKeyFile.Writer(directory, cfid, generation);
            }

            if (!commit.committed)
                writer.append(commit);
        }

        void mergeAndAppend(PaxosKeyState commit) throws IOException
        {
            while (true)
            {
                if (!iterator.hasNext())
                {
                    maybeAppend(commit);
                    return;
                }

                int cmp = KEY_COMPARATOR.compare(iterator.peek(), commit);

                if (cmp < 0)
                {
                    maybeAppend(iterator.next());
                }
                else if (cmp > 0)
                {
                    maybeAppend(commit);
                    return;
                }
                else
                {
                    maybeAppend(PaxosKeyState.merge(iterator.next(), commit));
                    return;
                }
            }
        }

        public UncommittedKeyFile finish() throws IOException
        {
            if (writer == null)
                return current;

            try
            {
                while (iterator.hasNext())
                    maybeAppend(iterator.next());
            }
            finally
            {
                iterator.close();
            }

            return writer.finish();
        }

        public Throwable abort(Throwable accumulate)
        {
            return writer.abort(accumulate);
        }
    }
}
