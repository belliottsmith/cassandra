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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public abstract class AbstractWriteResponseHandler<T> implements IAsyncCallbackWithFailure<T>
{
    protected static final Logger logger = LoggerFactory.getLogger( AbstractWriteResponseHandler.class );

    public final ConsistencyLevel consistencyLevel;
    protected final KeyspaceMetrics keyspaceMetrics;
    protected final long start;
    protected final Collection<InetAddress> naturalEndpoints;
    protected final Runnable callback;
    protected final Collection<InetAddress> pendingEndpoints;
    protected final WriteType writeType;
    protected final Predicate<InetAddress> isAlive;
    protected final AbstractReplicationStrategy replicationStrategySnapshot;
    //Count down until all responses and expirations have occured before deciding whether the ideal CL was reached.
    private AtomicInteger responsesAndExpirations;
    private final SimpleCondition condition = new SimpleCondition();
    private static final AtomicIntegerFieldUpdater<AbstractWriteResponseHandler> failuresUpdater
        = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, "failures");
    private volatile int failures = 0;


    /**
      * Delegate to another WriteReponseHandler or possibly this one to track if the ideal consistency level was reached.
      * Will be set to null if ideal CL was not configured
      * Will be set to an AWRH delegate if ideal CL was configured
      * Will be same as "this" if this AWRH is the ideal consistency level
      */
    private AbstractWriteResponseHandler idealCLDelegate;
    private boolean requestedCLAchieved = false;

    /**
     * @param callback A callback to be called when the write is successful.
     */
    protected AbstractWriteResponseHandler(KeyspaceMetrics keyspaceMetrics,
                                           AbstractReplicationStrategy replicationStrategySnapshot,
                                           Collection<InetAddress> naturalEndpoints,
                                           Collection<InetAddress> pendingEndpoints,
                                           ConsistencyLevel consistencyLevel,
                                           Runnable callback,
                                           WriteType writeType,
                                           Predicate<InetAddress> isAlive)
    {
        // It is not necessary to have the reference to keyspace.
        // Therefore, simply assign the metric and remove the keypsace reference as a class member.
        // The benifit of the removal is that compiler make sure there is no other usage of keyspace in the class and the derives.
        this.keyspaceMetrics = keyspaceMetrics;
        this.replicationStrategySnapshot = replicationStrategySnapshot;
        this.pendingEndpoints = pendingEndpoints;
        this.start = System.nanoTime();
        this.consistencyLevel = consistencyLevel;
        this.naturalEndpoints = naturalEndpoints;
        this.callback = callback;
        this.writeType = writeType;
        this.isAlive = isAlive;
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        long requestTimeout = writeType == WriteType.COUNTER
                            ? DatabaseDescriptor.getCounterWriteRpcTimeout()
                            : DatabaseDescriptor.getWriteRpcTimeout();

        long timeout = TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - start);

        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new WriteTimeoutException(writeType, consistencyLevel, acks, blockedFor);
        }

        if (totalBlockFor() + failures > totalEndpoints())
        {
            throw new WriteFailureException(consistencyLevel, ackCount(), failures, totalBlockFor(), writeType);
        }
    }

    /**
     * Set a delegate ideal CL write response handler. Note that this could be the same as this
     * if the ideal CL and requested CL are the same.
     */
    public void setIdealCLResponseHandler(AbstractWriteResponseHandler handler)
    {
        this.idealCLDelegate = handler;
        idealCLDelegate.responsesAndExpirations = new AtomicInteger(naturalEndpoints.size() + pendingEndpoints.size());
    }

    /**
     * This logs the response but doesn't do any further processing related to this write response handler
     * on whether the CL was achieved. Only call this after the subclass has completed all it's processing
     * since the subclass instance may be queried to find out if the CL was achieved.
     */
    protected final void logResponseToIdealCLDelegate(MessageIn<T> m)
    {
        //Tracking ideal CL was not configured
        if (idealCLDelegate == null)
        {
            return;
        }

        if (idealCLDelegate == this)
        {
            //Processing of the message was already done since this is the handler for the
            //ideal consistency level. Just decrement the counter.
            decrementResponseOrExpired();
        }
        else
        {
            //Let the delegate do full processing, this will loop back into the branch above
            //with idealCLDelegate == this, because the ideal write handler idealCLDelegate will always
            //be set to this in the delegate.
            idealCLDelegate.response(m);
        }
    }

    public final void expired()
    {
        //Tracking ideal CL was not configured
        if (idealCLDelegate == null)
        {
            return;
        }

        //The requested CL matched ideal CL so reuse this object
        if (idealCLDelegate == this)
        {
            decrementResponseOrExpired();
        }
        else
        {
            //Have the delegate track the expired response
            idealCLDelegate.decrementResponseOrExpired();
        }
    }


    /**
     * @return the minimum number of endpoints that must reply.
     */
    protected int totalBlockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return consistencyLevel.blockFor(replicationStrategySnapshot) + pendingEndpoints.size();
    }

    /** 
     * @return the total number of endpoints the request has been sent to. 
     */
    protected int totalEndpoints()
    {
        return naturalEndpoints.size() + pendingEndpoints.size();
    }

    /**
     * @return true if the message counts towards the totalBlockFor() threshold
     */
    protected boolean waitingFor(InetAddress from)
    {
        return true;
    }

    /**
     * @return number of responses received
     */
    protected abstract int ackCount();

    /** null message means "response from local write" */
    public abstract void response(MessageIn<T> msg);

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        // For writes we use totalBlockFor as any pending endpoints should also be considered
        // The exception is for EACH_QUORUM where the calculation has to be per-dc. See the
        // override in DatacenterSyncWriteResponseHandler for that.
        consistencyLevel.assureSufficientLiveNodes(replicationStrategySnapshot,
                                                   Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), isAlive),
                                                   totalBlockFor());
    }

    protected void signal()
    {
        //The ideal CL should only count as a strike if the requested CL was achieved.
        //If the requested CL is not achieved it's fine for the ideal CL to also not be achieved.
        if (idealCLDelegate != null)
        {
            idealCLDelegate.requestedCLAchieved = true;
        }

        condition.signalAll();
        if (callback != null)
            callback.run();
    }

    @Override
    public void onFailure(InetAddress from)
    {
        logger.trace("Got failure from {}", from);

        expired();

        int n = waitingFor(from)
                ? failuresUpdater.incrementAndGet(this)
                : failures;

        if (totalBlockFor() + n > totalEndpoints())
            signal();
    }

    /**
     * Decrement the counter for all responses/expirations and if the counter
     * hits 0 check to see if the ideal consistency level (this write response handler)
     * was reached using the signal.
     */
    private final void decrementResponseOrExpired()
    {
        int decrementedValue = responsesAndExpirations.decrementAndGet();
        if (decrementedValue == 0)
        {
            //The condition being signaled is a valid proxy for the CL being achieved
            //Only mark it as failed if the requested CL was achieved.
            if (!condition.isSignaled() & requestedCLAchieved)
            {
                keyspaceMetrics.writeFailedIdealCL.inc();
            }
            else
            {
                keyspaceMetrics.idealCLWriteLatency.addNano(System.nanoTime() - start);
            }

        }
    }
}
