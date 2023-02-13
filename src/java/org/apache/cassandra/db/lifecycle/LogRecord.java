/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.lifecycle;


import java.nio.file.Path;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A decoded line in a transaction log file replica.
 *
 * @see LogReplica and LogFile.
 */
final class LogRecord
{
    private static final Logger logger = LoggerFactory.getLogger(LogRecord.class);
    @VisibleForTesting
    static boolean INCLUDE_STATS_FOR_TESTS = false;

    public enum Type
    {
        UNKNOWN, // a record that cannot be parsed
        ADD,    // new files to be retained on commit
        REMOVE, // old files to be retained on abort
        COMMIT, // commit flag
        ABORT;  // abort flag

        public static Type fromPrefix(String prefix)
        {
            return valueOf(prefix.toUpperCase());
        }

        public boolean hasFile()
        {
            return this == Type.ADD || this == Type.REMOVE;
        }

        public boolean matches(LogRecord record)
        {
            return this == record.type;
        }

        public boolean isFinal()
        {
            return this == Type.COMMIT || this == Type.ABORT;
        }
    }

    /**
     * The status of a record after it has been verified, any parsing errors
     * are also store here.
     */
    public final static class Status
    {
        // if there are any errors, they end up here
        Optional<String> error = Optional.empty();

        // if the record was only partially matched across files this is true
        boolean partial = false;

        // if the status of this record on disk is required (e.g. existing files), it is
        // stored here for caching
        LogRecord onDiskRecord;

        void setError(String error)
        {
            if (!this.error.isPresent())
                this.error = Optional.of(error);
        }

        boolean hasError()
        {
            return error.isPresent();
        }
    }

    // the type of record, see Type
    public final Type type;
    // for sstable records, the absolute path of the table desc
    public final Optional<String> absolutePath;
    // for sstable records, the last update time of all files (may not be available for NEW records)
    public final long updateTime;
    // for sstable records, the total number of files (may not be accurate for NEW records)
    public final int numFiles;
    // the raw string as written or read from a file
    public final String raw;
    // the checksum of this record, written at the end of the record string
    public final long checksum;
    // the status of this record, @see Status class
    public final Status status;

    // (add|remove|commit|abort):[*,*,*][checksum]
    static Pattern REGEX = Pattern.compile("^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]\\[(\\d*)\\]$", Pattern.CASE_INSENSITIVE);

    public static LogRecord make(String line)
    {
        try
        {
            Matcher matcher = REGEX.matcher(line);
            if (!matcher.matches())
                return new LogRecord(Type.UNKNOWN, null, 0, 0, 0, line)
                       .setError(String.format("Failed to parse [%s]", line));

            Type type = Type.fromPrefix(matcher.group(1));
            return new LogRecord(type,
                                 matcher.group(2),
                                 Long.parseLong(matcher.group(3)),
                                 Integer.parseInt(matcher.group(4)),
                                 Long.parseLong(matcher.group(5)),
                                 line);
        }
        catch (IllegalArgumentException e)
        {
            return new LogRecord(Type.UNKNOWN, null, 0, 0, 0, line)
                   .setError(String.format("Failed to parse line: %s", e.getMessage()));
        }
    }

    public static LogRecord makeCommit(long updateTime)
    {
        return new LogRecord(Type.COMMIT, updateTime);
    }

    public static LogRecord makeAbort(long updateTime)
    {
        return new LogRecord(Type.ABORT, updateTime);
    }

    public static LogRecord make(Type type, SSTable table)
    {
        String absoluteTablePath = absolutePath(table.descriptor.baseFilename());
        return make(type, getExistingFiles(absoluteTablePath), table.getAllFilePaths().size(), absoluteTablePath);
    }

    public static Map<SSTable, LogRecord> make(Type type, Iterable<SSTableReader> tables)
    {
        // contains a mapping from sstable absolute path (everything up until the 'Data'/'Index'/etc part of the filename) to the sstable
        Map<String, SSTable> absolutePaths = new HashMap<>();
        for (SSTableReader table : tables)
            absolutePaths.put(absolutePath(table.descriptor.baseFilename()), table);

        // maps sstable base file name to the actual files on disk
        Map<String, List<File>> existingFiles = getExistingFiles(absolutePaths.keySet());
        Map<SSTable, LogRecord> records = new HashMap<>(existingFiles.size());
        for (Map.Entry<String, List<File>> entry : existingFiles.entrySet())
        {
            List<File> filesOnDisk = entry.getValue();
            String baseFileName = entry.getKey();
            SSTable sstable = absolutePaths.get(baseFileName);
            records.put(sstable, make(type, filesOnDisk, sstable.getAllFilePaths().size(), baseFileName));
        }
        return records;
    }

    private static String absolutePath(String baseFilename)
    {
        return FileUtils.getCanonicalPath(baseFilename + Component.separator);
    }

    public LogRecord withExistingFiles(List<File> existingFiles)
    {
        if (!absolutePath.isPresent())
        {
            throw new IllegalStateException(String.format("Cannot create record from existing files for type %s - file '%s' is not present",
                    type, absolutePath.get()));
        }
        return make(type, existingFiles, 0, absolutePath.get());
    }

    /**
     * We create a LogRecord based on the files on disk; there's some subtlety around how we handle stats files as the
     * timestamp can be mutated by the async completion of compaction if things race with node shutdown. To work around this,
     * we don't take the stats file timestamp into account when calculating nor using the timestamps for all the components
     * as we build the LogRecord.
     */
    public static LogRecord make(Type type, List<File> files, int minFiles, String absolutePath)
    {
        return make(type, files, minFiles, absolutePath, INCLUDE_STATS_FOR_TESTS);
    }

    /**
     * In most cases we skip including the stats file timestamp entirely as it can be mutated during anticompaction
     * and thus "invalidate" the LogRecord. There is an edge case where we have a LogRecord that was written w/the wrong
     * timestamp (i.e. included a mutated stats file) and we need the node to come up, so we need to expose the selective
     * ability to either include the stats file timestamp or not.
     *
     * See {@link LogFile#verifyRecord}
     */
    static LogRecord make(Type type, List<File> files, int minFiles, String absolutePath, boolean includeStatsFile)
    {
        List<File> toVerify;
        File statsFile = null;
        if (!includeStatsFile && !files.isEmpty())
        {
            toVerify = new ArrayList<>(files.size() - 1);
            for (File f : files)
            {
                if (Component.parseFromFullFileName(f.name()) == Component.STATS)
                    statsFile = f;
                else
                    toVerify.add(f);
            }
        }
        else
        {
            toVerify = files;
        }
        // CASSANDRA-11889: File.lastModified() returns a positive value only if the file exists, therefore
        // we filter by positive values to only consider the files that still exists right now, in case things
        // changed on disk since getExistingFiles() was called
        List<Long> positiveModifiedTimes = toVerify.stream().map(File::lastModified).filter(lm -> lm > 0).collect(Collectors.toList());
        long lastModified = positiveModifiedTimes.stream().reduce(0L, Long::max);
        if (statsFile != null && statsFile.lastModified() != lastModified)
            logger.warn("Found a {} file with a timestamp that doesn't match the LogRecord; this is likely due to a race " +
                        " in compaction changing mtime on the file. Ignoring mismatch so startup can continue. File: '{}'",
                        Component.STATS.name(), statsFile.name());

        // We need to preserve the file count for the number of existing files found on disk even though we ignored the
        // stats file during our timestamp calculation. If the stats file still exists, we add in the count of it as
        // a separate validation assumption that it's one of the files considered valid in this LogRecord.
        boolean addStatTS = statsFile != null && statsFile.exists();
        int positiveTSCount = addStatTS ? positiveModifiedTimes.size() + 1 : positiveModifiedTimes.size();
        return new LogRecord(type, absolutePath, lastModified, Math.max(minFiles, positiveTSCount));
    }

    private LogRecord(Type type, long updateTime)
    {
        this(type, null, updateTime, 0, 0, null);
    }

    private LogRecord(Type type,
                      String absolutePath,
                      long updateTime,
                      int numFiles)
    {
        this(type, absolutePath, updateTime, numFiles, 0, null);
    }

    private LogRecord(Type type,
                      String absolutePath,
                      long updateTime,
                      int numFiles,
                      long checksum,
                      String raw)
    {
        assert !type.hasFile() || absolutePath != null : "Expected file path for file records";

        this.type = type;
        this.absolutePath = type.hasFile() ? Optional.of(absolutePath) : Optional.<String>empty();
        this.updateTime = type == Type.REMOVE ? updateTime : 0;
        this.numFiles = type.hasFile() ? numFiles : 0;
        this.status = new Status();
        if (raw == null)
        {
            assert checksum == 0;
            this.checksum = computeChecksum();
            this.raw = format();
        }
        else
        {
            this.checksum = checksum;
            this.raw = raw;
        }
    }

    LogRecord setError(String error)
    {
        status.setError(error);
        return this;
    }

    String error()
    {
        return status.error.orElse("");
    }

    void setPartial()
    {
        status.partial = true;
    }

    boolean partial()
    {
        return status.partial;
    }

    boolean isValid()
    {
        return !status.hasError() && type != Type.UNKNOWN;
    }

    boolean isInvalid()
    {
        return !isValid();
    }

    boolean isInvalidOrPartial()
    {
        return isInvalid() || partial();
    }

    private String format()
    {
        return String.format("%s:[%s,%d,%d][%d]",
                             type.toString(),
                             absolutePath(),
                             updateTime,
                             numFiles,
                             checksum);
    }

    public static List<File> getExistingFiles(String absoluteFilePath)
    {
        File file = new File(absoluteFilePath);
        File[] files = file.parent().tryList((dir, name) -> name.startsWith(file.name()));
        // files may be null if the directory does not exist yet, e.g. when tracking new files
        return files == null ? Collections.emptyList() : Arrays.asList(files);
    }

    /**
     * absoluteFilePaths contains full file parts up to (but excluding) the component name
     *
     * This method finds all files on disk beginning with any of the paths in absoluteFilePaths
     *
     * @return a map from absoluteFilePath to actual file on disk.
     */
    public static Map<String, List<File>> getExistingFiles(Set<String> absoluteFilePaths)
    {
        Map<String, List<File>> fileMap = new HashMap<>();
        Map<File, TreeSet<String>> dirToFileNamePrefix = new HashMap<>();
        for (String absolutePath : absoluteFilePaths)
        {
            Path fullPath = new File(absolutePath).toPath();
            Path path = fullPath.getParent();
            if (path != null)
                dirToFileNamePrefix.computeIfAbsent(new File(path), (k) -> new TreeSet<>()).add(fullPath.getFileName().toString());
        }

        BiPredicate<File, String> ff = (dir, name) -> {
            TreeSet<String> dirSet = dirToFileNamePrefix.get(dir);
            // if the set contains a prefix of the current file name, the file name we have here should sort directly
            // after the prefix in the tree set, which means we can use 'floor' to get the prefix (returns the largest
            // of the smaller strings in the set). Also note that the prefixes always end with '-' which means we won't
            // have "xy-1111-Data.db".startsWith("xy-11") below (we'd get "xy-1111-Data.db".startsWith("xy-11-"))
            String baseName = dirSet.floor(name);
            if (baseName != null && name.startsWith(baseName))
            {
                String absolutePath = new File(dir, baseName).path();
                fileMap.computeIfAbsent(absolutePath, k -> new ArrayList<>()).add(new File(dir, name));
            }
            return false;
        };

        // populate the file map:
        for (File f : dirToFileNamePrefix.keySet())
            f.tryList(ff);

        return fileMap;
    }


    public boolean isFinal()
    {
        return type.isFinal();
    }

    String fileName()
    {
        return absolutePath.isPresent() ? new File(absolutePath.get()).name() : "";
    }

    boolean isInFolder(Path folder)
    {
        return absolutePath.isPresent() && PathUtils.isContained(folder, new File(absolutePath.get()).toPath());
    }

    String absolutePath()
    {
        return absolutePath.isPresent() ? absolutePath.get() : "";
    }

    @Override
    public int hashCode()
    {
        // see comment in equals
        return Objects.hash(type, absolutePath, numFiles, updateTime);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof LogRecord))
            return false;

        final LogRecord other = (LogRecord)obj;

        // we exclude on purpose checksum, error and full file path
        // since records must match across log file replicas on different disks
        return type == other.type &&
               absolutePath.equals(other.absolutePath) &&
               numFiles == other.numFiles &&
               updateTime == other.updateTime;
    }

    @Override
    public String toString()
    {
        return raw;
    }

    long computeChecksum()
    {
        CRC32 crc32 = new CRC32();
        crc32.update((absolutePath()).getBytes(FileUtils.CHARSET));
        crc32.update(type.toString().getBytes(FileUtils.CHARSET));
        FBUtilities.updateChecksumInt(crc32, (int) updateTime);
        FBUtilities.updateChecksumInt(crc32, (int) (updateTime >>> 32));
        FBUtilities.updateChecksumInt(crc32, numFiles);
        return crc32.getValue() & (Long.MAX_VALUE);
    }

    LogRecord asType(Type type)
    {
        return new LogRecord(type, absolutePath.orElse(null), updateTime, numFiles);
    }
}
