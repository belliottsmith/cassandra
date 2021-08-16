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
package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.db.partitions.PartitionUpdate;

public interface IMutation
{
    public long MAX_MUTATION_SIZE = DatabaseDescriptor.getMaxMutationSize();

    public String getKeyspaceName();
    public Collection<UUID> getColumnFamilyIds();
    public DecoratedKey key();
    public long getTimeout();
    public String toString(boolean shallow);
    public Collection<PartitionUpdate> getPartitionUpdates();

    /**
     * Validates size of mutation does not exceed {@link DatabaseDescriptor#getMaxMutationSize()}.
     *
     * @param version the MessagingService version the mutation is being serialized for.
     *                see {@link org.apache.cassandra.net.MessagingService#current_version}
     * @param overhead overhadd to add for mutation size to validate. Pass zero if not required but not a negative value.
     * @return size of the mutation (overhead is NOT included).
     * @throws MutationExceededMaxSizeException if {@link DatabaseDescriptor#getMaxMutationSize()} is exceeded
     */
    public void validateSize(int version, int overhead);
}
