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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;

public class PermissionsCache implements PermissionsCacheMBean, WarmableCache<Pair<AuthenticatedUser, IResource>, Set<Permission>>
{
    private static final Logger logger = LoggerFactory.getLogger(PermissionsCache.class);

    private final static String MBEAN_NAME = "org.apache.cassandra.auth:type=PermissionsCache";

    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("PermissionsCacheRefresh",
                                                                                             Thread.NORM_PRIORITY)
    {
        protected void afterExecute(Runnable r, Throwable t)
        {
            // overridden to avoid logging exceptions on background updates
            maybeResetTraceSessionWrapper(r);
        }
    };

    private final IAuthorizer authorizer;
    private volatile LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> cache;

    private volatile ScheduledFuture cacheRefresher = null;

    public PermissionsCache(IAuthorizer authorizer)
    {
        this.authorizer = authorizer;
        this.cache = initCache(null);
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    @VisibleForTesting
    protected static void unregisterMBean()
    {
        if (MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.unregisterMBean(MBEAN_NAME);
    }

    public Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        if (cache == null)
            return authorizer.authorize(user, resource);

        try
        {
            return cache.get(Pair.create(user, resource));
        }
        catch (ExecutionException | UncheckedExecutionException e)
        {
            Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
            throw new RuntimeException(e);
        }
    }

    public long size()
    {
        return cache == null ? 0L : cache.size();
    }

    public void invalidate()
    {
        cache = initCache(null);
        startActiveUpdate();
    }

    public void setValidity(int validityPeriod)
    {
        DatabaseDescriptor.setPermissionsValidity(validityPeriod);
        cache = initCache(cache);
        startActiveUpdate();
    }

    public int getValidity()
    {
        return DatabaseDescriptor.getPermissionsValidity();
    }

    public void setUpdateInterval(int updateInterval)
    {
        DatabaseDescriptor.setPermissionsUpdateInterval(updateInterval);
        cache = initCache(cache);
        startActiveUpdate();
    }


    public boolean getActiveUpdate()
    {
        return DatabaseDescriptor.getPermissionsCacheActiveUpdate();
    }

    public void setActiveUpdate(boolean update)
    {
        DatabaseDescriptor.setPermissionsCacheActiveUpdate(update);
    }

    public int getUpdateInterval()
    {
        return DatabaseDescriptor.getPermissionsUpdateInterval();
    }

    private LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> initCache(
                                                             LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> existing)
    {
        stopActiveUpdate();

        if (authorizer instanceof AllowAllAuthorizer)
            return null;

        if (DatabaseDescriptor.getPermissionsValidity() <= 0)
            return null;

        int validityPeriod = DatabaseDescriptor.getPermissionsValidity();
        int updateInterval = DatabaseDescriptor.getPermissionsUpdateInterval();
        boolean activeUpdate = DatabaseDescriptor.getPermissionsCacheActiveUpdate();

        LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> newcache = CacheBuilder.newBuilder()
                           .refreshAfterWrite(activeUpdate ? validityPeriod : updateInterval, TimeUnit.MILLISECONDS)
                           .expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                           .maximumSize(DatabaseDescriptor.getPermissionsCacheMaxEntries())
                           .build(new CacheLoader<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                           {
                               public Set<Permission> load(Pair<AuthenticatedUser, IResource> userResource)
                               {
                                   return authorizer.authorize(userResource.left, userResource.right);
                               }

                               public ListenableFuture<Set<Permission>> reload(final Pair<AuthenticatedUser, IResource> userResource,
                                                                               final Set<Permission> oldValue)
                               {
                                   ListenableFutureTask<Set<Permission>> task = ListenableFutureTask.create(new Callable<Set<Permission>>()
                                   {
                                       public Set<Permission>call() throws Exception
                                       {
                                           try
                                           {
                                               return authorizer.authorize(userResource.left, userResource.right);
                                           }
                                           catch (Exception e)
                                           {
                                               logger.trace("Error performing async refresh of user permissions", e);
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
            logger.info("Permissions cache not enabled, not starting active updates");
            return;
        }

        if (getActiveUpdate())
        {
            stopActiveUpdate();
            cacheRefresher = ScheduledExecutors.optionalTasks.scheduleAtFixedRate(CacheRefresher.create("permissions", cache),
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

    public void warm(Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>> entryProvider)
    {
        if (!DatabaseDescriptor.getAuthCacheWarmingEnabled())
        {
            logger.info("Prewarming of auth caches is disabled");
            return;
        }

        if (cache == null)
        {
            logger.info("Cache not enabled, skipping pre-warming");
            return;
        }

        int retries = Integer.getInteger("cassandra.permissions_cache.warming.max_retries", 10);
        long retryInterval = Long.getLong("cassandra.permissions_cache.warming.retry_interval_ms", 1000);

        while (retries-- > 0)
        {
            try
            {
                Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> entries = entryProvider.getInitialEntriesForCache();
                logger.info("Populating cache with {} pre-computed entries", entries.size());

                cache.putAll(entries);
                break;
            }
            catch (Exception e)
            {
                logger.info("Failed to pre-warm permissions cache, retrying {} more times", retries);
                logger.debug("Failed to pre-warm permissions cache", e);
                Uninterruptibles.sleepUninterruptibly(retryInterval, TimeUnit.MILLISECONDS);
            }
        }
    }
}
