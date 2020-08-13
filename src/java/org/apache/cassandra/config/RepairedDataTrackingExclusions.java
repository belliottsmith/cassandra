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

package org.apache.cassandra.config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.NoSpamLogger;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.cassandra.config.RepairedDataExclusion.*;

/**
 * Enables overrides to repaired data tracking to be configured at the keyspace, table and row level.
 * The first two levels are relatively self explanatory, we can specify that all rows in a given table
 * or in all tables for a given keyspace should be excluded from repaired data tracking. This will have
 * the effect that following an digest mismatch, reads to those tables will not contribute to the
 * repaired data digest compiled during the subsequent full data read. The replicas will still produce a
 * digest, which the coordinator will compare, but they will all be empty.
 *
 * The third case is more involved; in some cases we want to exclude specific rows from the digest, based
 * on their clustering values. More specifically a set of prefixes may be supplied to match against rows
 * being queried. Any row matching one of the prefixes will not contribute to the repaired data digest.
 * Prefixes are supplied as hex strings (more on the configuration later) and the matching process
 * consists of taking each prefix in turn, extracting the necessary number of bytes from the row's clustering
 * and checking for a match. Prefixes are checked in length order, to minimise the copying of bytes from
 * the clustering for comparison.
 *
 * In the primary use case for this the table contains a single clustering column of bytes type, which is
 * fairly straightforward. To handle composite clustering keys, we simply iterate through the clustering
 * values until we have enough bytes to match the prefix. This is the simplest approach, and works reasonably
 * for simple cases. However, as we don't take the length of each component into account, or delimit the
 * components in prefixes we can only greedily match. This is somewhat equivalent to only being able to
 * restrict a clustering component in CQL if all the preceding components are restricted.
 *
 * So for instance, given a clustering (bytes, text) the hex prefix 0x026161 would match rows with
 * all the following clusterings: (0x02:aa...), (0x0261, a...), (0x026161, ...), (0x026161..., ...)
 *
 * Numeric types also have complications, as the conversion to hex and comparison is less direct than
 * for text/bytes.
 * For example, an int value 1 has 4 byte binary representation 00000000 00000000 00000000 00000001
 * which in hex is 000011d7.
 * So a prefix specified as a hex string 000011d7 will match an int value 1, but also a long
 * value where the most significant bytes == 00000000 00000000 00000000 00000001
 * As we compare lexicographically, a short hex-formatted numeric prefix will match many more
 * potential values than string or plain byte types.
 * For example, a prefix 0000 will match any int < 65536 (which in hex is 00010000)
 *
 * Exclusions are configured via a JSON-escaped string in cassandra.yaml. It would be possible to
 * do this directly in the yaml, but this would require changes to cassandracfg in order to inject
 * the custom values.
 *
 * Example json:
 *
 * {
 *    "ks1": {
 *      "t1": ["02"],         // any rows in ks1.t1 with a clustering beginning '0x02' excluded
 *      "t99": []             // all rows in ks1.t99 excluded
 *    },
 *    "ks2": {}               // all tables/rows in ks2 excluded
 * }
 *
 * in cassandra.yaml:
 *
 * repaired_data_tracking_exclusions: " { \"ks1\": { \"t1\": [\"02\"], \"t99\": [] }, \"ks2\": {} }"
 */
public class RepairedDataTrackingExclusions
{
   private static final Logger logger = LoggerFactory.getLogger(RepairedDataTrackingExclusions.class);

   static final RepairedDataTrackingExclusions NO_EXCLUSIONS = new RepairedDataTrackingExclusions(Collections.emptyMap());

   @SuppressWarnings("unchecked")
   public static RepairedDataTrackingExclusions fromJsonString(String json)
   {
      if (Strings.isNullOrEmpty(json))
      {
         logger.info("No repaired data tracking exclusions specified in config");
         // no exclusions are specified in config, so we may as well ensure
         // that the feature is disabled and save some checks at read time
         DatabaseDescriptor.setRepairedDataTrackingExclusionsEnabled(false);
         return NO_EXCLUSIONS;
      }

      logger.info("Loading repaired data tracking exclusions from config");
      ObjectMapper mapper = new ObjectMapper();
      try
      {
         Object o = mapper.readValue(json, Object.class);
         return new RepairedDataTrackingExclusions((Map<String, Map<String, List<String>>>) o);
      }
      catch (IOException e)
      {
         throw new ConfigurationException("Failed to load repaired data tracking exclusions from config", e);
      }
   }

   private RepairedDataTrackingExclusions(Map<String, Map<String, List<String>>> mappings)
   {
      this.mappings = mappings;
   }

   // Raw data from yaml
   private final Map<String, Map<String, List<String>>> mappings;

   public RepairedDataExclusion getExclusion(String keyspace, String table)
   {
      Map<String, List<String>> keyspaceExclusions = mappings.get(keyspace);
      if (keyspaceExclusions == null)
      {
         // Nothing in the exclusions config for this keyspace
         return NONE;
      }
      else if (keyspaceExclusions.isEmpty())
      {
         // The keyspace is in config, but no specific tables, so every table is implicitly excluded
         return ALL;
      }

      // The keyspace has some specific tables excluded
      List<String> tableExclusions = keyspaceExclusions.get(table);
      if (tableExclusions == null)
      {
         // nothing for this specific table, so nothing is excluded
         return NONE;
      }
      else if (tableExclusions.isEmpty())
      {
         //No clustering prefixes specified, so exclude everything in the table
         return ALL;
      }
      else
      {
         // Some prefixes are configured for this table, so we'll need to check per-row
         // we can cache the exclusion prefixes for the table though
         return new ExcludeSome(new PrefixExclusion(tableExclusions));
      }
   }

   public String toString()
   {
      return "RepairedDataTrackingExclusions{" +
             "mappings=" + mappings +
             '}';
   }

   private static class PrefixExclusion implements Predicate<Clustering>
   {
      final int longest;
      private final byte[][] prefixes;

      private PrefixExclusion(Collection<String> prefixes)
      {
         int idx = 0;
         this.prefixes = new byte[prefixes.size()][];
         int tmp = 0;
         for (String s : prefixes)
         {
            try
            {
               byte[] b = Hex.hexToBytes(s);
               tmp = Math.max(tmp, b.length);
               this.prefixes[idx++] = b;
            }
            catch (Exception e)
            {
               NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                1L, TimeUnit.MINUTES,
                                "Invalid hex string for exclusion prefix ({})", s);
            }
         }
         longest = tmp;

         // Sort prefixes by length to minimize copying from clustering values
         Arrays.sort(this.prefixes, Comparator.comparingInt(b -> b.length));
      }

      public boolean apply(Clustering clustering)
      {
         final byte[] buffer = new byte[longest];
         if (clustering.size() == 0)
            return false;

         ByteBuffer[] values = clustering.getRawValues();
         int bufferPos = 0;
         int componentIdx = 0;
         int componentPos = 0;

         // Iterate the prefixes in length order. For each one, check if there are enough bytes
         // in the comparison buffer to match against the prefix. If not, try and copy just
         // enough bytes to do so. If there are multiple clustering components move through them
         // until we have enough bytes for the comparison.
         for(byte[] prefix : prefixes)
         {
            int required = prefix.length - bufferPos;
            while (required > 0 && componentIdx < values.length)
            {
               if (values[componentIdx].remaining() - componentPos >= required)
               {
                  FastByteOperations.copy(values[componentIdx], componentPos, buffer, bufferPos, required);
                  componentPos += required;
                  bufferPos += required;
               }
               else
               {
                  int toRead = values[componentIdx].remaining() - componentPos;
                  if (toRead > 0)
                     FastByteOperations.copy(values[componentIdx], componentPos, buffer, bufferPos, toRead);
                  componentIdx++;
                  componentPos = 0;
                  bufferPos += toRead;
               }
               required = prefix.length - bufferPos;
            }

            // if the total number of bytes in the clustering is less than in the prefix,
            // then it can't possibly be a match so move onto the next prefix
            if (bufferPos < prefix.length)
               continue;

            int cmp = FastByteOperations.compareUnsigned(prefix, 0, prefix.length, buffer, 0, prefix.length);
            if (cmp == 0)
               return true;
         }
         return false;
      }
   }
}
