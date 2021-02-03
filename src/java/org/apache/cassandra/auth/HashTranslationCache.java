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

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;
import org.mindrot.jbcrypt.BCrypt;

public class HashTranslationCache implements HashTranslationCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(HashTranslationCache.class);
    private final static String MBEAN_NAME = "org.apache.cassandra.auth:type=HashTranslationCache";
    private static final long MISMATCH_PAUSE_MS = Long.getLong("cassandra.hash_cache_fail_pause_ms", 100);
    private static final int INITIAL_MAX = Integer.getInteger("cassandra.hash_cache_max_size_default", 128);

    public static final int FAST_HASH_ROUNDS = Integer.getInteger("cassandra.hash_cache_rounds", 4);
    private static final String CACHE_GEN_SALT = BCrypt.gensalt(FAST_HASH_ROUNDS);

    private int maxSize;

    protected Cache<String, String> hashCache;

    public HashTranslationCache()
    {
        this(true, INITIAL_MAX);
    }

    @VisibleForTesting
    HashTranslationCache(boolean registerMBean, int maxSize)
    {
        this.maxSize = maxSize;
        buildCache();
        if (registerMBean)
        {
            // If already exists then replace previous with this one. ie if PasswordAuthenticator gets reinstantiated
            // somehow with new feature to change at runtime or SchemaLoader.setupAuth re-called.
            if (MBeanWrapper.instance.isRegistered(MBEAN_NAME))
                MBeanWrapper.instance.unregisterMBean(MBEAN_NAME);
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
        }
    }

    @Override
    public long getHits() {
        return hashCache.stats().hitCount();
    }

    @Override
    public long getMisses() {
        return hashCache.stats().missCount();
    }

    @Override
    public void setCacheMaxSize(int size)
    {
        this.maxSize = size;
        buildCache();
    }

    @Override
    public int getCacheMaxSize() {
        return maxSize;
    }

    @Override
    public boolean getHashCacheEnabled()
    {
        return DatabaseDescriptor.getAuthHashCacheEnabled();
    }

    @Override
    public void setHashCacheEnabled(boolean enabled)
    {
        DatabaseDescriptor.setAuthHashCacheEnabled(enabled);
    }

    private void buildCache()
    {
        this.hashCache = CacheBuilder.newBuilder()
                                     .maximumSize(maxSize)
                                     .recordStats()
                                     .build();
    }

    /**
     * Get the bcrypted hash of `password` given the salt and rounds of target `hash`. This will cache and translate
     * to a 2^FAST_HASH_ROUNDS round bcrypted hash if its more expensive. In that case however will also add artificial
     * non cpu intensive pause if they do not match. In the case of an invalid hash or error this will log the issue and
     * return null.
     *
     * @param password cleartext password
     * @param hash bcrypted hash that this will be compared against
     * @return bcrypted password
     */
    @Nullable
    public String getHash(String password, String hash)
    {
        try
        {
            int rounds = getRounds(hash);
            // only use cache if > 2^FAST_HASH_ROUNDS rounds and enabled
            if (DatabaseDescriptor.getAuthHashCacheEnabled() && rounds > FAST_HASH_ROUNDS) {
                String bcrypted = hashCache.get(BCrypt.hashpw(password + hash, CACHE_GEN_SALT),
                                                () -> BCrypt.hashpw(password, hash));
                if (!bcrypted.equals(hash))
                    Uninterruptibles.sleepUninterruptibly(MISMATCH_PAUSE_MS, TimeUnit.MILLISECONDS);

                return bcrypted;
            }
        }
        catch (Exception e)
        {
            logger.error("Hash cache failure: ", e);
        }

        // fallback if disabled, already at FAST_HASH_ROUNDS rounds or some unexpected error occurs
        try
        {
            return BCrypt.hashpw(password, hash);
        }
        catch (Exception e)
        {
            // Improperly formatted hashes may cause BCrypt.checkpw to throw, so trap any other exception as a failure
            logger.warn("Error: invalid password hash encountered, rejecting user", e);
            return null;
        }
    }

    /**
     * Get rounds of a bcrypt hash in format of `$minorversion$rounds$salt+hash`. If it fails to parse or if it
     * has invalid number of rounds (only 4-31 allowed, enforced in jbcrypt) will throw IllegalArgumentException
     * @param bcryptHash
     * @throws IllegalArgumentException
     * @return rounds of a bcrypt hash
     */
    public static int getRounds(String bcryptHash) throws IllegalArgumentException
    {
        int rounds = -1;
        try
        {
            String[] split = bcryptHash.split("\\$");
            rounds = Integer.parseInt(split[2]);
        } catch (Exception e)
        {
            throw new IllegalArgumentException("Invalid hash: " + bcryptHash, e);
        }
        if (rounds >= 4 && rounds <= 31)
            return rounds;

        throw new IllegalArgumentException("Invalid hash: " + bcryptHash);
    }
}
