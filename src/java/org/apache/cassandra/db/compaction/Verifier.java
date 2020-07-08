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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.FileDigestValidator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

public class Verifier implements Closeable
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;

    private final CompactionController controller;


    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final VerifyInfo verifyInfo;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final Options options;
    private final boolean isOffline;
    /**
     * Given a keyspace, return the set of local and pending token ranges.  By default {@link StorageService#getLocalAndPendingRanges(String)}
     * is expected, but for the standalone verifier case we can't use that, so this is here to allow the CLI to provide
     * the token ranges.
     */
    private final Function<String, ? extends Collection<Range<Token>>> tokenLookup;

    private int goodRows;

    private final OutputHandler outputHandler;
    private FileDigestValidator validator;

    public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, boolean isOffline, Options options)
    {
        this(cfs, sstable, new OutputHandler.LogOutput(), isOffline, options);
    }

    public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata, sstable.descriptor, sstable.header);

        this.controller = new VerifyController(cfs);

        this.dataFile = isOffline
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());
        this.indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)));
        this.verifyInfo = new VerifyInfo(dataFile, sstable);
        this.options = options;
        this.isOffline = isOffline;
        this.tokenLookup = options.tokenLookup;
    }

    public void verify()
    {
        boolean extended = options.extendedVerification;
        long rowStart = 0;

        outputHandler.output(String.format("Verifying %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length())));
        if (options.checkVersion && !sstable.descriptor.version.isLatestVersion())
        {
            String msg = String.format("%s is not the latest version, run upgradesstables", sstable);
            outputHandler.output(msg);
            // don't use markAndThrow here because we don't want a CorruptSSTableException for this.
            throw new RuntimeException(msg);
        }
        outputHandler.output(String.format("Deserializing sstable metadata for %s ", sstable));
        try
        {
            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            Map<MetadataType, MetadataComponent> sstableMetadata = sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, types);
            if (sstableMetadata.containsKey(MetadataType.VALIDATION) &&
                !((ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION)).partitioner.equals(sstable.getPartitioner().getClass().getCanonicalName()))
                throw new IOException("Partitioner does not match validation metadata");
        }
        catch (Throwable t)
        {
            outputHandler.warn(t.getMessage());
            markAndThrow(false);
        }

        try
        {
            outputHandler.debug("Deserializing index for "+sstable);
            deserializeIndex(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.warn(t.getMessage());
            markAndThrow();
        }

        try
        {
            outputHandler.debug("Deserializing index summary for "+sstable);
            deserializeIndexSummary(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.output("Index summary is corrupt - if it is removed it will get rebuilt on startup "+sstable.descriptor.filenameFor(Component.SUMMARY));
            outputHandler.warn(t.getMessage());
            markAndThrow(false);
        }

        try
        {
            outputHandler.debug("Deserializing bloom filter for "+sstable);
            deserializeBloomFilter(sstable);

        }
        catch (Throwable t)
        {
            outputHandler.warn(t.getMessage());
            markAndThrow();
        }

        if (options.checkOwnsTokens && !isOffline)
        {
            outputHandler.debug("Checking that all tokens are owned by the current node");
            try (KeyIterator iter = new KeyIterator(sstable.descriptor, sstable.metadata))
            {
                List<Range<Token>> ownedRanges = Range.normalize(tokenLookup.apply(cfs.metadata.ksName));
                if (ownedRanges.isEmpty())
                    return;
                RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
                while (iter.hasNext())
                {
                    DecoratedKey key = iter.next();
                    rangeOwnHelper.check(key);
                }
            }
            catch (Throwable t)
            {
                outputHandler.warn(t.getMessage());
                markAndThrow();
            }
        }

        if (options.quick)
            return;

        // Verify will use the Digest files, which works for both compressed and uncompressed sstables
        outputHandler.output(String.format("Checking computed hash of %s ", sstable));
        try
        {
            validator = null;

            if (sstable.descriptor.digestComponent != null &&
                new File(sstable.descriptor.filenameFor(sstable.descriptor.digestComponent)).exists())
            {
                validator = DataIntegrityMetadata.fileDigestValidator(sstable.descriptor);
                validator.validate();
            }
            else
            {
                outputHandler.output("Data digest missing, assuming extended verification of disk values");
                extended = true;
            }
        }
        catch (IOException e)
        {
            outputHandler.warn(e.getMessage());
            markAndThrow();
        }
        finally
        {
            FileUtils.closeQuietly(validator);
        }

        if (!extended)
            return;

        outputHandler.output("Extended Verify requested, proceeding to inspect values");

        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserialize(indexFile, nextIndexKey).position;
                if (firstRowPositionFromIndex != 0)
                    markAndThrow();
            }

            List<Range<Token>> ownedRanges = isOffline ? Collections.emptyList() : Range.normalize(tokenLookup.apply(cfs.metadata.ksName));
            RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {

                if (verifyInfo.isStopRequested())
                    throw new CompactionInterruptedException(verifyInfo.getCompactionInfo());

                rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                if (options.checkOwnsTokens && ownedRanges.size() > 0)
                {
                    try
                    {
                        rangeOwnHelper.check(key);
                    }
                    catch (Throwable t)
                    {
                        outputHandler.warn(String.format("Key %s in sstable %s not owned by local ranges %s", key, sstable, ownedRanges), t);
                        markAndThrow();
                    }
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex = 0;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF()
                                             ? dataFile.length()
                                             : rowIndexEntrySerializer.deserialize(indexFile, nextIndexKey).position;
                }
                catch (Throwable th)
                {
                    markAndThrow();
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining();

                long dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug(String.format("row %s is %s bytes", keyName, dataSize));

                assert currentIndexKey != null || indexFile.isEOF();

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow();

                    try (UnfilteredRowIterator iterator = new SSTableIdentityIterator(sstable, dataFile, key))
                    {
                        Row first = null;
                        int duplicateRows = 0;
                        long minTimestamp = Long.MAX_VALUE;
                        long maxTimestamp = Long.MIN_VALUE;
                        while (iterator.hasNext())
                        {
                            Unfiltered uf = iterator.next();
                            if (uf.isRow())
                            {
                                Row row = (Row) uf;
                                if (first != null && first.clustering().equals(row.clustering()))
                                {
                                    duplicateRows++;
                                    for (Cell cell : row.cells())
                                    {
                                        maxTimestamp = Math.max(cell.timestamp(), maxTimestamp);
                                        minTimestamp = Math.min(cell.timestamp(), minTimestamp);
                                    }
                                }
                                else
                                {
                                    if (duplicateRows > 0)
                                        logDuplicates(key, first, duplicateRows, minTimestamp, maxTimestamp);
                                    duplicateRows = 0;
                                    first = row;
                                    maxTimestamp = Long.MIN_VALUE;
                                    minTimestamp = Long.MAX_VALUE;
                                }
                            }
                        }
                        if (duplicateRows > 0)
                            logDuplicates(key, first, duplicateRows, minTimestamp, maxTimestamp);
                    }

                    if ( (prevKey != null && prevKey.compareTo(key) > 0) || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex )
                        markAndThrow();

                    goodRows++;
                    prevKey = key;


                    outputHandler.debug(String.format("Row %s at %s valid, moving to next row at %s ", goodRows, rowStart, nextRowPositionFromIndex));
                    dataFile.seek(nextRowPositionFromIndex);
                }
                catch (Throwable th)
                {
                    markAndThrow();
                }
            }
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();
        }

        outputHandler.output("Verify of " + sstable + " succeeded. All " + goodRows + " rows read successfully");
    }

    private void logDuplicates(DecoratedKey key, Row first, int duplicateRows, long minTimestamp, long maxTimestamp)
    {
        String keyString = sstable.metadata.getKeyValidator().getString(key.getKey());
        long firstMaxTs = Long.MIN_VALUE;
        long firstMinTs = Long.MAX_VALUE;
        for (Cell cell : first.cells())
        {
            firstMaxTs = Math.max(firstMaxTs, cell.timestamp());
            firstMinTs = Math.min(firstMinTs, cell.timestamp());
        }
        outputHandler.output(String.format("%d duplicate rows found for [%s %s] in %s.%s (%s), timestamps: [first row (%s, %s)], [duplicates (%s, %s, eq:%b)]",
                                           duplicateRows,
                                           keyString, first.clustering().toString(sstable.metadata),
                                           sstable.metadata.ksName,
                                           sstable.metadata.cfName,
                                           sstable,
                                           dateString(firstMinTs), dateString(firstMaxTs),
                                           dateString(minTimestamp), dateString(maxTimestamp), minTimestamp == maxTimestamp));
    }

    private String dateString(long time)
    {
        return Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(time)).toString();
    }

    /**
     * Use the fact that check(..) is called with sorted tokens - we keep a pointer in to the normalized ranges
     * and only bump the pointer if the key given is out of range. This is done to avoid calling .contains(..) many
     * times for each key (with vnodes for example)
     */
    @VisibleForTesting
    public static class RangeOwnHelper
    {
        private final List<Range<Token>> normalizedRanges;
        private int rangeIndex = 0;
        private DecoratedKey lastKey;

        public RangeOwnHelper(List<Range<Token>> normalizedRanges)
        {
            this.normalizedRanges = normalizedRanges;
        }

        /**
         * check if the given key is contained in any of the given ranges
         *
         * Must be called in sorted order - key should be increasing
         *
         * @param key the key
         * @throws RuntimeException if the key is not contained
         */
        public void check(DecoratedKey key)
        {
            assert lastKey == null || key.compareTo(lastKey) > 0;
            lastKey = key;

            if (normalizedRanges.isEmpty()) // handle tests etc where we don't have any ranges
                return;

            if (rangeIndex > normalizedRanges.size() - 1)
                throw new IllegalStateException("RangeOwnHelper can only be used to find the first out-of-range-token");

            while (!normalizedRanges.get(rangeIndex).contains(key.getToken()))
            {
                rangeIndex++;
                if (rangeIndex > normalizedRanges.size() - 1)
                    throw new RuntimeException("Key "+key+" is not contained in the given ranges");
            }
        }
    }

    private void deserializeIndex(SSTableReader sstable) throws IOException
    {
        try (RandomAccessReader primaryIndex = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX))))
        {
            long indexSize = primaryIndex.length();

            while ((primaryIndex.getFilePointer()) != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                RowIndexEntry.Serializer.skip(primaryIndex, sstable.descriptor.version);
            }
        }
    }

    private void deserializeIndexSummary(SSTableReader sstable) throws IOException
    {
        File file = new File(sstable.descriptor.filenameFor(Component.SUMMARY));
        CFMetaData metadata = cfs.metadata;
        try (DataInputStream iStream = new DataInputStream(Files.newInputStream(file.toPath())))
        {
            try (IndexSummary indexSummary = IndexSummary.serializer.deserialize(iStream,
                                                               cfs.getPartitioner(),
                                                               sstable.descriptor.version.hasSamplingLevel(),
                                                               metadata.params.minIndexInterval,
                                                               metadata.params.maxIndexInterval))
            {
                ByteBufferUtil.readWithLength(iStream);
                ByteBufferUtil.readWithLength(iStream);
            }
        }
    }

    private void deserializeBloomFilter(SSTableReader sstable) throws IOException
    {
        try (DataInputStream stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(sstable.descriptor.filenameFor(Component.FILTER)))));
             IFilter bf = FilterFactory.deserialize(stream, true, sstable.descriptor.version.hasOldBfHashOrder()))
        {}
    }

    public void close()
    {
        FileUtils.closeQuietly(dataFile);
        FileUtils.closeQuietly(indexFile);
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void markAndThrow()
    {
        markAndThrow(true);
    }

    private void markAndThrow(boolean mutateRepaired)
    {
        if (mutateRepaired && options.mutateRepairStatus) // if we are able to mutate repaired flag, an incremental repair should be enough
        {
            try
            {
                sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().pendingRepair);
                sstable.reloadSSTableMetadata();
                cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
            }
            catch(IOException ioe)
            {
                outputHandler.output("Error mutating repairedAt for SSTable " +  sstable.getFilename() + ", as part of markAndThrow");
            }
        }
        Exception e = new Exception(String.format("Invalid SSTable %s, please force %srepair", sstable.getFilename(), (mutateRepaired && options.mutateRepairStatus) ? "" : "a full "));
        if (options.invokeDiskFailurePolicy)
            throw new CorruptSSTableException(e, sstable.getFilename());
        else
            throw new RuntimeException(e);
    }

    public CompactionInfo.Holder getVerifyInfo()
    {
        return verifyInfo;
    }

    private static class VerifyInfo extends CompactionInfo.Holder
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        private final UUID verificationCompactionId;

        public VerifyInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            verificationCompactionId = UUIDGen.getTimeUUID();
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.VERIFY,
                                          dataFile.getFilePointer(),
                                          dataFile.length(),
                                          verificationCompactionId,
                                          ImmutableSet.of(sstable));
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }

        public boolean isGlobal()
        {
            return false;
        }
    }

    private static class VerifyController extends CompactionController
    {
        public VerifyController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public Predicate<Long> getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }

    public static Options.Builder options()
    {
        return new Options.Builder();
    }

    public static class Options
    {
        public final boolean invokeDiskFailurePolicy;
        public final boolean extendedVerification;
        public final boolean checkVersion;
        public final boolean mutateRepairStatus;
        public final boolean checkOwnsTokens;
        public final boolean quick;
        public final Function<String, ? extends Collection<Range<Token>>> tokenLookup;

        private Options(boolean invokeDiskFailurePolicy, boolean extendedVerification, boolean checkVersion, boolean mutateRepairStatus, boolean checkOwnsTokens, boolean quick, Function<String, ? extends Collection<Range<Token>>> tokenLookup)
        {
            this.invokeDiskFailurePolicy = invokeDiskFailurePolicy;
            this.extendedVerification = extendedVerification;
            this.checkVersion = checkVersion;
            this.mutateRepairStatus = mutateRepairStatus;
            this.checkOwnsTokens = checkOwnsTokens;
            this.quick = quick;
            this.tokenLookup = tokenLookup;
        }

        @Override
        public String toString()
        {
            return "Options{" +
                   "invokeDiskFailurePolicy=" + invokeDiskFailurePolicy +
                   ", extendedVerification=" + extendedVerification +
                   ", checkVersion=" + checkVersion +
                   ", mutateRepairStatus=" + mutateRepairStatus +
                   ", checkOwnsTokens=" + checkOwnsTokens +
                   ", quick=" + quick +
                   '}';
        }

        public static class Builder
        {
            private boolean invokeDiskFailurePolicy = false; // invoking disk failure policy can stop the node if we find a corrupt stable
            private boolean extendedVerification = false;
            private boolean checkVersion = false;
            private boolean mutateRepairStatus = false; // mutating repair status can be dangerous
            private boolean checkOwnsTokens = false;
            private boolean quick = false;
            private Function<String, ? extends Collection<Range<Token>>> tokenLookup = StorageService.instance::getLocalAndPendingRanges;

            public Builder invokeDiskFailurePolicy(boolean param)
            {
                this.invokeDiskFailurePolicy = param;
                return this;
            }

            public Builder extendedVerification(boolean param)
            {
                this.extendedVerification = param;
                return this;
            }

            public Builder checkVersion(boolean param)
            {
                this.checkVersion = param;
                return this;
            }

            public Builder mutateRepairStatus(boolean param)
            {
                this.mutateRepairStatus = param;
                return this;
            }

            public Builder checkOwnsTokens(boolean param)
            {
                this.checkOwnsTokens = param;
                return this;
            }

            public Builder quick(boolean param)
            {
                this.quick = param;
                return this;
            }

            public Builder tokenLookup(Function<String, ? extends Collection<Range<Token>>> tokenLookup)
            {
                this.tokenLookup = tokenLookup;
                return this;
            }

            public Options build()
            {
                return new Options(invokeDiskFailurePolicy, extendedVerification, checkVersion, mutateRepairStatus, checkOwnsTokens, quick, tokenLookup);
            }

        }
    }
}
