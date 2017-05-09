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

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.WrappedRunnable;

public class AsyncRepairCallback implements IAsyncCallback<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( AsyncRepairCallback.class );

    private final DataResolver repairResolver;
    private final int blockfor;
    protected final AtomicInteger received = new AtomicInteger(0);

    public AsyncRepairCallback(DataResolver repairResolver, int blockfor)
    {
        this.repairResolver = repairResolver;
        this.blockfor = blockfor;
    }

    public void response(MessageIn<ReadResponse> message)
    {
        repairResolver.preprocess(message);
        if (received.incrementAndGet() == blockfor)
        {
            StageManager.getStage(Stage.READ_REPAIR).execute(new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    try
                    {
                        repairResolver.compareResponses();
                    }
                    catch (ReadTimeoutException e)
                    {
                        ReadRepairMetrics.timedOut.mark();
                        if (logger.isDebugEnabled() )
                            logger.debug("Timed out merging read repair responses", e);
                    }
                }
            });
        }
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }
}
