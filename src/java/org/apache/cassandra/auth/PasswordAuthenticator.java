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

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.auth.CassandraRoleManager.consistencyForRoleForRead;

/**
 * PasswordAuthenticator is an IAuthenticator implementation
 * that keeps credentials (rolenames and bcrypt-hashed passwords)
 * internally in C* - in system_auth.roles CQL3 table.
 * Since 2.2, the management of roles (creation, modification,
 * querying etc is the responsibility of IRoleManager. Use of
 * PasswordAuthenticator requires the use of CassandraRoleManager
 * for storage and retrieval of encrypted passwords.
 */
public class PasswordAuthenticator implements IAuthenticator, Cacheable<String, String>
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);

    // name of the hash column.
    private static final String SALTED_HASH = "salted_hash";

    // really this is a rolename now, but as it only matters for Thrift, we leave it for backwards compatibility
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";

    static final byte NUL = 0;
    private SelectStatement authenticateStatement;

    public static final String LEGACY_CREDENTIALS_TABLE = "credentials";
    private SelectStatement legacyAuthenticateStatement;

    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("AuthenticatorCacheRefresh",
                                                                                             Thread.NORM_PRIORITY)
    {
        protected void afterExecute(Runnable r, Throwable t)
        {
            // overridden to avoid logging exceptions on background updates
            maybeResetTraceSessionWrapper(r);
        }
    };

    private final CredentialsCache cache = new CredentialsCache();

    // sentinel value indicating that the password hash has been deleted
    private static final String DELETED_HASH_SENTINEL = "";

    protected static final HashTranslationCache hashCache = new HashTranslationCache();

    public void warmCache()
    {
        cache.warm(this);
    }

    public void startCacheActiveUpdate()
    {
        cache.startActiveUpdate();
    }

    @Override
    public Map<String, String> getInitialEntriesForCache()
    {
        Map<String, String> entries = new HashMap<>();

        if (Schema.instance.getCFMetaData(AuthKeyspace.NAME, LEGACY_CREDENTIALS_TABLE) != null)
        {
            logger.info("Pre-warming credentials cache from legacy credentials table");
            UntypedResultSet results = QueryProcessor.process("SELECT username, salted_hash FROM system_auth.credentials",
                                                              consistencyForRoleForRead());
            results.forEach(row -> entries.put(row.getString("username"), row.getString("salted_hash")));
        }
        else
        {
            logger.info("Pre-warming credentials cache from roles table");
            UntypedResultSet results = QueryProcessor.process("SELECT role, salted_hash FROM system_auth.roles",
                                                              consistencyForRoleForRead());
            results.forEach(row -> {
                if (row.has("salted_hash"))
                {
                    entries.put(row.getString("role"), row.getString("salted_hash"));
                }
            });
        }
        return entries;
    }

    private String getPasswordHash(String username) throws AuthenticationException
    {
        try
        {
            SelectStatement authStmt = authenticationStatement();

            UntypedResultSet result;
            try
            {
                ResultMessage.Rows rows = authStmt.execute(QueryState.forInternalCalls(),
                                                           QueryOptions.forInternalCalls(consistencyForRoleForRead(),
                                                                                         Lists.newArrayList(ByteBufferUtil.bytes(username))));
                result = UntypedResultSet.create(rows.result);
            }
            catch (RequestValidationException e)
            {
                // needs to be caught here because AuthenticationException is a subclass of RequestValidationException
                // and we don't want to throw an assertion error on invalid username/password
                throw new AssertionError(e); // not supposed to happen
            }

            if (result.isEmpty() || !result.one().has(SALTED_HASH))
                throw new AuthenticationException("Username and/or password are incorrect");
            else
                return result.one().getString(SALTED_HASH);
        }
        catch (RequestExecutionException e)
        {
            logger.trace("Error performing internal authentication", e);
            throw new AuthenticationException(String.format("%s - caused by user: %s", e.toString(), username));
        }
    }

    // No anonymous access.
    public boolean requireAuthentication()
    {
        return true;
    }

    protected static boolean checkpw(String password, String hash)
    {
        String bcrypted = hashCache.getHash(password, hash);
        return bcrypted != null && hash.compareTo(bcrypted) == 0;
    }

    private AuthenticatedUser authenticate(String username, String password) throws AuthenticationException
    {
        try
        {
            String storedPasswordHash = cache.get(username);

            if (storedPasswordHash == DELETED_HASH_SENTINEL)
                cache.invalidate(username);

            if (storedPasswordHash == null || !checkpw(password, storedPasswordHash))
                throw new AuthenticationException("Username and/or password are incorrect");

            return new AuthenticatedUser(username);
        }
        catch (UncheckedExecutionException | ExecutionException e)
        {
            Throwables.propagateIfInstanceOf(e.getCause(), AuthenticationException.class);
            throw new AuthenticationException("Unable to perform authentication: " + e.getMessage(), e);
        }
    }

    /**
     * If the legacy users table exists try to verify credentials there. This is to handle the case
     * where the cluster is being upgraded and so is running with mixed versions of the authn tables
     */
    private SelectStatement authenticationStatement()
    {
        if (Schema.instance.getCFMetaData(AuthKeyspace.NAME, LEGACY_CREDENTIALS_TABLE) == null)
            return authenticateStatement;
        else
        {
            // If the credentials was initialised only after statement got prepared, re-prepare (CASSANDRA-12813).
            if (legacyAuthenticateStatement == null)
                prepareLegacyAuthenticateStatement();
            return legacyAuthenticateStatement;
        }
    }

    public Set<DataResource> protectedResources()
    {
        // Also protected by CassandraRoleManager, but the duplication doesn't hurt and is more explicit
        return ImmutableSet.of(DataResource.table(AuthKeyspace.NAME, AuthKeyspace.ROLES));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        String query = String.format("SELECT %s FROM %s.%s WHERE role = ?",
                                     SALTED_HASH,
                                     AuthKeyspace.NAME,
                                     AuthKeyspace.ROLES);
        authenticateStatement = prepare(query);

        if (Schema.instance.getCFMetaData(AuthKeyspace.NAME, LEGACY_CREDENTIALS_TABLE) != null)
            prepareLegacyAuthenticateStatement();
    }

    private void prepareLegacyAuthenticateStatement()
    {
        String query = String.format("SELECT %s from %s.%s WHERE username = ?",
                                     SALTED_HASH,
                                     AuthKeyspace.NAME,
                                     LEGACY_CREDENTIALS_TABLE);
        legacyAuthenticateStatement = prepare(query);
    }

    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
    {
        String username = credentials.get(USERNAME_KEY);
        if (username == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing", USERNAME_KEY));

        String password = credentials.get(PASSWORD_KEY);
        if (password == null)
            throw new AuthenticationException(String.format("Required key '%s' is missing - caused by user: %s", PASSWORD_KEY, username));

        return authenticate(username, password);
    }

    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return new PlainTextSaslAuthenticator();
    }

    private SelectStatement prepare(String query)
    {
        return (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
    }

    private class PlainTextSaslAuthenticator implements SaslNegotiator
    {
        private boolean complete = false;
        private String username;
        private String password;

        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
        {
            decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        public boolean isComplete()
        {
            return complete;
        }

        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException
        {
            if (!complete)
                throw new AuthenticationException("SASL negotiation not complete");
            return authenticate(username, password);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}
         * authzId is optional, and in fact we don't care about it here as we'll
         * set the authzId to match the authnId (that is, there is no concept of
         * a user being authorized to act on behalf of another with this IAuthenticator).
         *
         * @param bytes encoded credentials string sent by the client
         * @return map containing the username/password pairs in the form an IAuthenticator
         * would expect
         * @throws javax.security.sasl.SaslException
         */
        private void decodeCredentials(byte[] bytes) throws AuthenticationException
        {
            logger.trace("Decoding credentials from client token");
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            for (int i = bytes.length - 1; i >= 0; i--)
            {
                if (bytes[i] == NUL)
                {
                    if (pass == null)
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    else if (user == null)
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    else
                        throw new AuthenticationException("Credential format error: username or password is empty or contains NUL(\\0) character");

                    end = i;
                }
            }

            if (user == null || user.length == 0)
                throw new AuthenticationException("Authentication ID must not be null");

            username = new String(user, StandardCharsets.UTF_8);

            if (pass == null || pass.length == 0)
                throw new AuthenticationException(String.format("Password must not be null - caused by user: %s", username));


            password = new String(pass, StandardCharsets.UTF_8);
        }
    }

    // Not much more than a wrapper around Guava's loading cache. In upstream versions >= 3.4, this is properly
    // generalized by CASSANDRA-7715. It's done here so we can make it a WarmableCache and prepopulate it at startup
    private class CredentialsCache implements WarmableCache<String, String>
    {
        private volatile ScheduledFuture cacheRefresher = null;
        private final LoadingCache<String, String> cache = initCache(DatabaseDescriptor.getAuthenticatorValidity(),
                                                                     DatabaseDescriptor.getAuthenticatorUpdateInterval(),
                                                                     DatabaseDescriptor.getAuthenticatorCacheMaxEntries());

        private String get(String username) throws ExecutionException
        {
            if (cache == null)
                return getPasswordHash(username);

            return cache.get(username);
        }

        private void invalidate(String username)
        {
            if (cache != null)
                cache.invalidate(username);
        }

        private LoadingCache<String, String> initCache(int validityPeriod,
                                                       int updateInterval,
                                                       int maxEntries)
        {
            if (validityPeriod <= 0)
            {
                logger.info("Authenticator cache is disabled");
                return null;
            }

            boolean activeUpdate = DatabaseDescriptor.getAuthenticatorCacheActiveUpdate();

            LoadingCache<String, String> newcache = CacheBuilder.newBuilder()
                                                                .refreshAfterWrite(activeUpdate ? validityPeriod : updateInterval, TimeUnit.MILLISECONDS)
                                                                .expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                                                                .maximumSize(maxEntries)
                                                                .build(new CacheLoader<String, String>()
                                                                {
                                                                    public String load(String username) throws Exception
                                                                    {
                                                                        try
                                                                        {
                                                                            return getPasswordHash(username);
                                                                        }
                                                                        catch (AuthenticationException e)
                                                                        {
                                                                            throw e;
                                                                        }
                                                                    }

                                                                    public ListenableFuture<String> reload(final String username,
                                                                                                           final String oldValue)
                                                                    {
                                                                        ListenableFutureTask<String> task = ListenableFutureTask.create(new Callable<String>()
                                                                        {
                                                                            public String call() throws Exception
                                                                            {
                                                                                try
                                                                                {
                                                                                    return getPasswordHash(username);
                                                                                }
                                                                                catch (AuthenticationException e)
                                                                                {
                                                                                    return DELETED_HASH_SENTINEL;
                                                                                }
                                                                                catch (Exception e)
                                                                                {
                                                                                    logger.debug("Error performing async refresh of authenticator", e);
                                                                                    throw e;
                                                                                }
                                                                            }
                                                                        });
                                                                        cacheRefreshExecutor.execute(task);
                                                                        return task;
                                                                    }
                                                                });
            return newcache;
        }

        public void startActiveUpdate()
        {
            if (cache == null)
            {
                logger.info("Credentials cache not enabled, not starting active updates");
                return;
            }

            if (DatabaseDescriptor.getAuthenticatorCacheActiveUpdate())
            {
                stopActiveUpdate();
                cacheRefresher = ScheduledExecutors.optionalTasks.scheduleAtFixedRate(CacheRefresher.create("authenticator", cache, DELETED_HASH_SENTINEL),
                                                                                      DatabaseDescriptor.getAuthenticatorUpdateInterval(),
                                                                                      DatabaseDescriptor.getAuthenticatorUpdateInterval(),
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

        public void warm(Cacheable<String, String> entryProvider)
        {
            if (!DatabaseDescriptor.getAuthCacheWarmingEnabled())
            {
                logger.info("Prewarming of auth caches is disabled");
                return;
            }

            if (cache == null)
            {
                logger.info("Cache not enabled, skipping pre-warming");
                return ;
            }

            int retries = Integer.getInteger("cassandra.credentials_cache.warming.max_retries", 10);
            long retryInterval = Long.getLong("cassandra.credentials_cache.warming.retry_interval_ms", 1000);

            while (retries-- > 0)
            {
                try
                {
                    Map<String, String> entries = entryProvider.getInitialEntriesForCache();
                    logger.info("Populating cache with {} pre-computed entries", entries.size());
                    cache.putAll(entries);
                    break;
                }
                catch (Exception e)
                {
                    logger.info("Failed to pre-warm credentials cache, retrying {} more times", retries);
                    logger.debug("Failed to pre-warm credentials cache", e);
                    Uninterruptibles.sleepUninterruptibly(retryInterval, TimeUnit.MILLISECONDS);
                }
            }
        }
    }
}
