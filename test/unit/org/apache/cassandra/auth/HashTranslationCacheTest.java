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


import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.mindrot.jbcrypt.BCrypt.gensalt;
import static org.mindrot.jbcrypt.BCrypt.hashpw;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class HashTranslationCacheTest
{
    private HashTranslationCache cache;
    private static boolean authHashCacheEnabled;

    @BeforeClass
    public static void pre()
    {
        // in case default changes this wont break tests assumptions
        authHashCacheEnabled = DatabaseDescriptor.getAuthHashCacheEnabled();
        DatabaseDescriptor.setAuthHashCacheEnabled(true);
    }

    @AfterClass
    public static void post()
    {
        DatabaseDescriptor.setAuthHashCacheEnabled(authHashCacheEnabled);
    }

    @Before
    public void freshCache()
    {
        cache = new HashTranslationCache(false, 128);
    }

    @Test
    public void testHashCacheHits() throws Exception
    {
        String expensive = hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(31));

        assertEquals(cache.getHits(), 0);
        assertEquals(expensive, cache.getHash(DEFAULT_SUPERUSER_PASSWORD, expensive));
        assertEquals(cache.getHits(), 0);
        assertEquals(cache.getMisses(), 1);

        assertEquals(expensive, cache.getHash(DEFAULT_SUPERUSER_PASSWORD, expensive));
        assertEquals(cache.getHits(), 1);
        assertEquals(cache.getMisses(), 1);
    }

    @Test
    public void testHashCacheDontCacheDefaultSized() throws Exception
    {
        String cheap = hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(HashTranslationCache.FAST_HASH_ROUNDS));

        assertEquals(cache.getHits(), 0);
        assertEquals(cache.getMisses(), 0);
        assertEquals(cheap, cache.getHash(DEFAULT_SUPERUSER_PASSWORD, cheap));
        assertEquals(cache.getHits(), 0);
        assertEquals(cache.getMisses(), 0);
    }

    @Test
    public void testHashCacheDisabled() throws Exception
    {
        String expensive = hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(31));

        assertEquals(cache.getHits(), 0);
        assertEquals(expensive, cache.getHash(DEFAULT_SUPERUSER_PASSWORD, expensive));
        assertEquals(cache.getHits(), 0);
        assertEquals(cache.getMisses(), 1);

        DatabaseDescriptor.setAuthHashCacheEnabled(false);
        assertEquals(expensive, cache.getHash(DEFAULT_SUPERUSER_PASSWORD, expensive));
        assertEquals(cache.getHits(), 0);
        assertEquals(cache.getMisses(), 1);
        DatabaseDescriptor.setAuthHashCacheEnabled(true);
    }

    @Test
    public void testMBeanNameCollision() throws Exception
    {
        // make sure multiple registers doesnt error out
        new HashTranslationCache(true, 10);
        // We could verify the latest takes place of first, but requires MBeanWrapper changes and
        // register/unregister already shown to work
        new HashTranslationCache(true, 10);
    }

    @Test
    public void testParseBcryptRounds() throws Exception
    {
        // note: doing all range 4-31 will spin CPU for insanely long time so only doing subset
        qt().withExamples(5)
            .forAll(integers().between(4, 15))
            .check((i) ->
                   HashTranslationCache.getRounds(hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(i))) == i);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseBcryptRoundsNull() throws Exception
    {
        HashTranslationCache.getRounds(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseBcryptRoundsInvalidRange2() throws Exception
    {
        HashTranslationCache.getRounds("$2a$02$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseBcryptRoundsInvalidRange32() throws Exception
    {
        HashTranslationCache.getRounds("$2a$32$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseBcryptRoundsInvalidHash() throws Exception
    {
        HashTranslationCache.getRounds("abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseBcryptRoundsEmptyString() throws Exception
    {
        HashTranslationCache.getRounds("");
    }
}
