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
package org.apache.cassandra.net.async;

import java.io.EOFException;
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;

import org.apache.cassandra.io.util.RebufferingInputStream;
import org.jctools.queues.SpscUnboundedArrayQueue;

import static org.apache.cassandra.net.async.FrameDecoder.*;

class AsyncInputPlus extends RebufferingInputStream
{
    /**
     * Exception thrown when closure was explicitly requested.
     */
    static final class InputClosedException extends EOFException
    {
    }

    // EMPTY_BUFFER is used to signal AsyncInputPlus that it should be closed
    private static final Slice CLOSE_INPUT = Slice.EMPTY;

    private final Queue<Slice> queue;

    private final IntConsumer onReleased;

    private Slice slice;
    private int sliceSize;

    private volatile boolean isClosed;
    private volatile Thread parkedThread;

    AsyncInputPlus(IntConsumer onReleased)
    {
        super(Slice.EMPTY.contents);
        this.slice = Slice.EMPTY;
        this.sliceSize = 0;

        this.queue = new SpscUnboundedArrayQueue<>(16);
        this.onReleased = onReleased;
    }

    @Override
    protected void reBuffer() throws InputClosedException
    {
        releaseCurrentBuf();

        Slice nextSlice = pollBlockingly();
        if (nextSlice == CLOSE_INPUT)
        {
            isClosed = true;
            throw new InputClosedException();
        }

        slice = nextSlice;
        sliceSize = nextSlice.contents.remaining();
        buffer = nextSlice.contents;
    }

    @Override
    public void close()
    {
        if (isClosed)
            return;

        if (null != slice)
            releaseCurrentBuf();

        Slice nextSlice;
        while ((nextSlice = pollBlockingly()) != CLOSE_INPUT)
        {
            onReleased.accept(nextSlice.readableBytes());
            nextSlice.release();
        }

        isClosed = true;
    }

    void supply(Slice slice)
    {
        if (isClosed)
            throw new IllegalStateException("Cannot supply a buffer to a closed AsyncInputPlus");

        queue.add(slice);
        maybeUnpark();
    }

    private void releaseCurrentBuf()
    {
        slice.release();
        if (sliceSize > 0)
            onReleased.accept(sliceSize);
        slice = null;
        sliceSize = 0;
        buffer = null;
    }

    void requestClosure()
    {
        if (isClosed)
            throw new IllegalStateException("Cannot close an already closed AsyncInputPlus");

        queue.add(CLOSE_INPUT);
        maybeUnpark();
    }

    private Slice pollBlockingly()
    {
        Slice buf = queue.poll();
        if (null != buf)
            return buf;

        parkedThread = Thread.currentThread();
        while ((buf = queue.poll()) == null)
            LockSupport.park();
        parkedThread = null;
        return buf;
    }

    private void maybeUnpark()
    {
        Thread thread = parkedThread;
        if (null != thread)
            LockSupport.unpark(thread);
    }
}
