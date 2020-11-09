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
import java.util.Collection;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Handles flushing files, and the state changes associated with them.
 */
class UncommittedKeyFileContainer
{
    private final UUID cfid;
    private final File directory;
    private volatile FileState fileState;

    public UncommittedKeyFileContainer(UUID cfid, File directory, UncommittedKeyFile tableFile)
    {
        this.cfid = cfid;
        this.directory = directory;
        this.fileState = new FileState(tableFile);
    }

    static UncommittedKeyFileContainer load(File directory, UUID cfid)
    {
        return new UncommittedKeyFileContainer(cfid, directory, UncommittedKeyFile.cleanupAndLoadMostRecent(directory, cfid));
    }

    CloseableIterator<PaxosKeyState> iterator(Collection<Range<Token>> ranges)
    {
        while (true)
        {
            FileState current = fileState;

            // no file yet, just return an empty iterator
            if (current.file == null)
                return CloseableIterator.empty();

            CloseableIterator<PaxosKeyState> iterator = current.file.iterator(ranges);

            // if we grabbed the file state right before the current file was marked for
            // deletion, we'll get null, so just try again
            if (iterator == null)
                continue;

            return iterator;
        }
    }

    private synchronized void verifyAndSetFileState(FileState expected, FileState update)
    {
        Preconditions.checkState(fileState == expected, "expected file state %s, but saw %s", expected, fileState);
        fileState = update;
    }

    FlushWriter createFlushWriter()
    {
        FileState current = fileState;
        Preconditions.checkState(!(current instanceof FlushingFileState), "paxos state flush already in progress for %s", cfid);
        FlushingFileState flushingState = new FlushingFileState(current.file);
        verifyAndSetFileState(current, flushingState);
        return flushingState;
    }

    @VisibleForTesting
    UncommittedKeyFile getCurrentFile()
    {
        return fileState.file;
    }

    synchronized void truncate()
    {
        fileState.truncate();
    }

    public interface FlushWriter
    {
        void update(PaxosKeyState commitState) throws IOException;
        long finish() throws IOException;
        Throwable abort(Throwable accumulate);
    }

    private static class FileState
    {
        final UncommittedKeyFile file;

        FileState(UncommittedKeyFile file)
        {
            this.file = file;
        }

        void truncate()
        {
            file.markDeleted();
        }
    }

    private class FlushingFileState extends FileState implements FlushWriter
    {
        private final UncommittedKeyFile.MergeWriter writer;
        private boolean finished = false;
        private long size = -1;

        public FlushingFileState(UncommittedKeyFile file)
        {
            super(file);
            this.writer = new UncommittedKeyFile.MergeWriter(cfid, directory, file);
        }

        public void update(PaxosKeyState commitState) throws IOException
        {
            writer.mergeAndAppend(commitState);
        }

        public synchronized long finish() throws IOException
        {
            if (finished)
                return size;
            size = file != null ? file.sizeOnDisk() : 0;
            verifyAndSetFileState(this, new FileState(writer.finish()));
            if (file != null)
                file.markDeleted();
            finished = true;
            return size;
        }

        public synchronized Throwable abort(Throwable accumulate)
        {
            if (finished)
                return accumulate;
            Throwable t = writer.abort(accumulate);
            verifyAndSetFileState(this, new FileState(file));
            finished = true;
            return t;
        }

        void truncate()
        {
            Throwable t = abort(null);
            if (t != null)
                throw new RuntimeException(t);
            super.truncate();
        }
    }
}
