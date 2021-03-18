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

package org.apache.cassandra.auth;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

public class NetworkAuthCache implements WarmableCache<RoleResource, DCPermissions>, NetworkAuthCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(PermissionsCache.class);

    private final static String MBEAN_NAME = "org.apache.cassandra.auth:type=NetworkAuthCache";

    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("NetworkAuthCacheRefresh",
                                                                                             Thread.NORM_PRIORITY)
    {
        protected void afterExecute(Runnable r, Throwable t)
        {
            // overridden to avoid logging exceptions on background updates
            maybeResetTraceSessionWrapper(r);
        }
    };

    private final INetworkAuthorizer authorizer;
    private volatile LoadingCache<RoleResource, DCPermissions> cache;

    private volatile ScheduledFuture cacheRefresher = null;

    public NetworkAuthCache(INetworkAuthorizer authorizer)
    {
        this.authorizer = authorizer;
        this.cache = initCache(null, authorizer.requireAuthorization());
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    @VisibleForTesting
    protected static void unregisterMBean()
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            logger.warn("Error unregistering Permissions Cache mbean", e);
        }
    }

    public DCPermissions getPermissions(RoleResource role)
    {
        if (cache == null)
            return authorizer.authorize(role);

        try
        {
            return cache.get(role);
        }
        catch (ExecutionException | UncheckedExecutionException e)
        {
            Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
            throw new RuntimeException(e);
        }
    }

    public void invalidate()
    {
        cache = initCache(null, DatabaseDescriptor.getAuthenticator().requireAuthentication());
        startActiveUpdate();
    }

    public void setValidity(int validityPeriod)
    {
        DatabaseDescriptor.setRolesValidity(validityPeriod);
        cache = initCache(cache, DatabaseDescriptor.getAuthenticator().requireAuthentication());
        startActiveUpdate();
    }

    public int getValidity()
    {
        return DatabaseDescriptor.getRolesValidity();
    }

    public void setUpdateInterval(int updateInterval)
    {
        DatabaseDescriptor.setRolesUpdateInterval(updateInterval);
        cache = initCache(cache, DatabaseDescriptor.getAuthenticator().requireAuthentication());
        startActiveUpdate();
    }

    public int getUpdateInterval()
    {
        return DatabaseDescriptor.getRolesUpdateInterval();
    }

    public boolean getActiveUpdate()
    {
        return DatabaseDescriptor.getRolesCacheActiveUpdate();
    }

    public long size()
    {
        return cache == null ? 0L : cache.size();
    }

    private LoadingCache<RoleResource, DCPermissions> initCache(LoadingCache<RoleResource, DCPermissions> existing,
                                                            boolean requireAuthentication)
    {

        stopActiveUpdate();

        if (!requireAuthentication)
            return null;

        if (DatabaseDescriptor.getRolesValidity() <= 0)
            return null;

        int validityPeriod = DatabaseDescriptor.getRolesValidity();
        int updateInterval = DatabaseDescriptor.getRolesUpdateInterval();
        boolean activeUpdate = DatabaseDescriptor.getRolesCacheActiveUpdate();

        LoadingCache<RoleResource, DCPermissions> newcache = CacheBuilder.newBuilder()
                                                                         .refreshAfterWrite(activeUpdate ? validityPeriod : updateInterval, TimeUnit.MILLISECONDS)
                                                                         .expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                                                                         .maximumSize(DatabaseDescriptor.getRolesCacheMaxEntries())
                                                                         .build(new CacheLoader<RoleResource, DCPermissions>()
                                                                     {
                                                                         public DCPermissions load(RoleResource primaryRole)
                                                                         {
                                                                             return authorizer.authorize(primaryRole);
                                                                         }

                                                                         public ListenableFuture<DCPermissions> reload(final RoleResource primaryRole,
                                                                                                                       final DCPermissions oldValue)
                                                                         {
                                                                             ListenableFutureTask<DCPermissions> task;
                                                                             task = ListenableFutureTask.create(new Callable<DCPermissions>()
                                                                             {
                                                                                 public DCPermissions call() throws Exception
                                                                                 {
                                                                                     try
                                                                                     {
                                                                                         return authorizer.authorize(primaryRole);
                                                                                     } catch (Exception e)
                                                                                     {
                                                                                         logger.trace("Error performing async refresh of user roles", e);
                                                                                         throw e;
                                                                                     }
                                                                                 }
                                                                             });
                                                                             cacheRefreshExecutor.execute(task);
                                                                             return task;
                                                                         }
                                                                     });
        if (existing != null)
            newcache.putAll(existing.asMap());

        return newcache;
    }

    public void startActiveUpdate()
    {
        if (cache == null)
        {
            logger.info("Network permissions cache not enabled, not starting active updates");
            return;
        }

        if (getActiveUpdate())
        {
            stopActiveUpdate();
            cacheRefresher = ScheduledExecutors.optionalTasks.scheduleAtFixedRate(CacheRefresher.create("network permissions", cache),
                                                                                  getUpdateInterval(),
                                                                                  getUpdateInterval(),
                                                                                  TimeUnit.MILLISECONDS);
        }
    }

    public void stopActiveUpdate()
    {
        if (cacheRefresher != null)
        {
            cacheRefresher.cancel(false);
            cacheRefresher = null;
        }
    }

    public void warm(Cacheable<RoleResource, DCPermissions> entryProvider)
    {
        if (!DatabaseDescriptor.getAuthCacheWarmingEnabled())
        {
            logger.info("Prewarming of auth caches is disabled");
            return;
        }

        if (cache == null)
        {
            logger.info("Network permissions cache not enabled, skipping pre-warming");
            return;
        }

        int retries = Integer.getInteger("cassandra.roles_cache.warming.max_retries", 10);
        long retryInterval = Long.getLong("cassandra.roles_cache.warming.retry_interval_ms", 1000);

        while (retries-- > 0)
        {
            try
            {
                Map<RoleResource, DCPermissions> entries = entryProvider.getInitialEntriesForCache();
                logger.info("Populating cache with {} pre-computed entries", entries.size());
                cache.putAll(entries);
                break;
            }
            catch (Exception e)
            {
                logger.info("Failed to pre-warm network permissions cache, retrying {} more times", retries);
                logger.debug("Failed to pre-warm network permissions cache", e);
                Uninterruptibles.sleepUninterruptibly(retryInterval, TimeUnit.MILLISECONDS);
            }
        }
    }
}
