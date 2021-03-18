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
import java.util.Set;
import java.util.concurrent.*;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class RolesCache implements RolesCacheMBean, WarmableCache<RoleResource, Set<Role>>
{
    private static final Logger logger = LoggerFactory.getLogger(RolesCache.class);

    private final static String MBEAN_NAME = "org.apache.cassandra.auth:type=RolesCache";
    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("RolesCacheRefresh",
                                                                                             Thread.NORM_PRIORITY)
    {
        protected void afterExecute(Runnable r, Throwable t)
        {
            // overridden to empty to avoid logging on background updates
            maybeResetTraceSessionWrapper(r);
        }
    };

    private final IRoleManager roleManager;
    private volatile LoadingCache<RoleResource, Set<Role>> cache;

    private volatile ScheduledFuture cacheRefresher = null;

    public RolesCache(IRoleManager roleManager)
    {
        this(roleManager, DatabaseDescriptor.getAuthenticator().requireAuthentication());
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    @VisibleForTesting
    public RolesCache(IRoleManager roleManager, boolean enableCache)
    {
        this.roleManager = roleManager;
        this.cache = initCache(null, enableCache);
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
            logger.warn("Error unregistering Roles Cache mbean", e);
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

    /**
     * Read or return from the cache the Set of the RoleResources identifying the roles granted to the primary resource
     * @see Roles#getRoles(RoleResource)
     * @param primaryRole identifier for the primary role
     * @return the set of identifiers of all the roles granted to (directly or through inheritance) the primary role
     */
    Set<RoleResource> getRoleResources(RoleResource primaryRole)
    {
        if (cache == null)
            return roleManager.getRoles(primaryRole, true);

        try
        {
            return Sets.newHashSet(Iterables.transform(cache.get(primaryRole), r -> r.resource));
        }
        catch (ExecutionException | UncheckedExecutionException e)
        {
            Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
            throw new RuntimeException(e);
        }
    }

    /**
     * Read or return from cache the set of Role objects representing the roles granted to the primary resource
     * @see Roles#getGrantedRoles(RoleResource)
     * @param primaryRole identifier for the primary role
     * @return the set of Role objects containing info of all roles granted to (directly or through inheritance)
     * the primary role.
     */
    Set<Role> getRoles(RoleResource primaryRole)
    {
        if (cache == null)
            return roleManager.getGrantedRoles(primaryRole);

        try
        {
            return cache.get(primaryRole);
        }
        catch (ExecutionException | UncheckedExecutionException e)
        {
            Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
            throw new RuntimeException(e);
        }
    }

    Set<RoleResource> getAllRoles()
    {
        // this method seems kind of unnecessary as it is only called from Roles::getAllRoles,
        // but we are able to inject the RoleManager to this class, making testing possible. If
        // we lose this method and did everything in Roles, we'd be dependent on the IRM impl
        // supplied by DatabaseDescriptor
        return roleManager.getAllRoles();
    }

    public long size()
    {
        return cache == null ? 0L : cache.size();
    }

    private LoadingCache<RoleResource, Set<Role>> initCache(LoadingCache<RoleResource, Set<Role>> existing,
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

        LoadingCache<RoleResource, Set<Role>> newcache = CacheBuilder.newBuilder()
                .refreshAfterWrite(activeUpdate ? validityPeriod : updateInterval, TimeUnit.MILLISECONDS)
                .expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                .maximumSize(DatabaseDescriptor.getRolesCacheMaxEntries())
                .build(new CacheLoader<RoleResource, Set<Role>>()
                {
                    public Set<Role> load(RoleResource primaryRole)
                    {
                        return roleManager.getGrantedRoles(primaryRole);
                    }

                    public ListenableFuture<Set<Role>> reload(final RoleResource primaryRole,
                                                              final Set<Role> oldValue)
                    {
                        ListenableFutureTask<Set<Role>> task;
                        task = ListenableFutureTask.create(new Callable<Set<Role>>()
                        {
                            public Set<Role> call() throws Exception
                            {
                                try
                                {
                                    return roleManager.getGrantedRoles(primaryRole);
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
            logger.info("Roles cache not enabled, not starting active updates");
            return;
        }

        if (getActiveUpdate())
        {
            stopActiveUpdate();
            cacheRefresher = ScheduledExecutors.optionalTasks.scheduleAtFixedRate(CacheRefresher.create("roles", cache),
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

    public void warm(Cacheable<RoleResource, Set<Role>> entryProvider)
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

        int retries = Integer.getInteger("cassandra.roles_cache.warming.max_retries", 10);
        long retryInterval = Long.getLong("cassandra.roles_cache.warming.retry_interval_ms", 1000);

        while (retries-- > 0)
        {
            try
            {
                Map<RoleResource, Set<Role>> entries = entryProvider.getInitialEntriesForCache();
                logger.info("Populating cache with {} pre-computed entries", entries.size());
                cache.putAll(entries);
                break;
            }
            catch (Exception e)
            {
                logger.info("Failed to pre-warm roles cache, retrying {} more times", retries);
                logger.debug("Failed to pre-warm roles cache", e);
                Uninterruptibles.sleepUninterruptibly(retryInterval, TimeUnit.MILLISECONDS);
            }
        }
    }
}
