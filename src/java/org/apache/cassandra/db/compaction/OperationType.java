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
package org.apache.cassandra.db.compaction;

public enum OperationType
{
    COMPACTION("Compaction", true),
    VALIDATION("Validation", false),
    KEY_CACHE_SAVE("Key cache save", false),
    ROW_CACHE_SAVE("Row cache save", false),
    COUNTER_CACHE_SAVE("Counter cache save", false),
    CLEANUP("Cleanup", true),
    SCRUB("Scrub", true),
    UPGRADE_SSTABLES("Upgrade sstables", true),
    INDEX_BUILD("Secondary index build", false),
    /** Compaction for tombstone removal */
    TOMBSTONE_COMPACTION("Tombstone Compaction", true),
    UNKNOWN("Unknown compaction type", false),
    ANTICOMPACTION("Anticompaction after repair", true),
    VERIFY("Verify", false),
    FLUSH("Flush", true),
    STREAM("Stream", true),
    WRITE("Write", true),
    VIEW_BUILD("View build", false),
    INDEX_SUMMARY("Index summary redistribution", false);

    public final String type;
    public final String fileName;
    public final boolean writesData;

    OperationType(String type, boolean writesData)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
        this.writesData = writesData;
    }

    public static OperationType fromFileName(String fileName)
    {
        for (OperationType opType : OperationType.values())
            if (opType.fileName.equals(fileName))
                return opType;

        throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
    }

    public String toString()
    {
        return type;
    }
}
