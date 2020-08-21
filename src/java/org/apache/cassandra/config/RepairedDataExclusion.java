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

import com.google.common.base.Predicate;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;

public interface RepairedDataExclusion
{
    boolean excludePartition(UnfilteredRowIterator partition);
    boolean excludeRow(Row row);

    /**
     * Filter out any excluded rows - used by preview repair.
     */
    default UnfilteredRowIterator filter(UnfilteredRowIterator partition)
    {
        return Transformation.apply(partition, new Transformation<BaseRowIterator<?>>()
        {
            protected Row applyToRow(Row row)
            {
                if (excludeRow(row))
                    return null;
                return super.applyToRow(row);
            }
        });
    }

    static boolean exclusionsEnabled() { return DatabaseDescriptor.getRepairedDataTrackingExclusionsEnabled(); }

    static final RepairedDataExclusion NONE = new RepairedDataExclusion()
    {
        public boolean excludePartition(UnfilteredRowIterator partition) { return false; }
        public boolean excludeRow(Row row) { return false; }
    };

    static final RepairedDataExclusion ALL = new RepairedDataExclusion()
    {
        public boolean excludePartition(UnfilteredRowIterator partition) { return exclusionsEnabled(); }
        public boolean excludeRow(Row row) { return exclusionsEnabled(); }
    };

    static class ExcludeSome implements  RepairedDataExclusion
    {
        private final Predicate<Clustering> exclusions;

        public ExcludeSome(Predicate<Clustering> exclusions)
        {
            this.exclusions = exclusions;
        }

        public boolean excludePartition(UnfilteredRowIterator partition) { return false; }

        public boolean excludeRow(Row row)
        {
            return exclusionsEnabled() && exclusions.apply(row.clustering());
        }
    }
}
