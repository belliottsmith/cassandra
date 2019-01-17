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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.management.*;
import javax.management.openmbean.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.Counter;
import org.apache.cassandra.cache.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.db.lifecycle.*;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.xmas.BoundsAndOldestTombstone;
import org.apache.cassandra.db.xmas.InvalidatedRepairedRange;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TableMetrics.Sampler;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.TopKSampler.SamplerResult;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    // The directories which will be searched for sstables on cfs instantiation.
    private static volatile Directories.DataDirectory[] initialDirectories = Directories.dataDirectories;

    /**
     * A hook to add additional directories to initialDirectories.
     * Any additional directories should be added prior to ColumnFamilyStore instantiation on startup
     *
     * Since the directories used by a given table are determined by the compaction strategy,
     * it's possible for sstables to be written to directories specified outside of cassandra.yaml.
     * By adding additional directories to initialDirectories, sstables in these extra locations are
     * made discoverable on sstable instantiation.
     */
    public static synchronized void addInitialDirectories(Directories.DataDirectory[] newDirectories)
    {
        assert newDirectories != null;

        Set<Directories.DataDirectory> existing = Sets.newHashSet(initialDirectories);

        List<Directories.DataDirectory> replacementList = Lists.newArrayList(initialDirectories);
        for (Directories.DataDirectory directory: newDirectories)
        {
            if (!existing.contains(directory))
            {
                replacementList.add(directory);
            }
        }

        Directories.DataDirectory[] replacementArray = new Directories.DataDirectory[replacementList.size()];
        replacementList.toArray(replacementArray);
        initialDirectories = replacementArray;
    }

    public static Directories.DataDirectory[] getInitialDirectories()
    {
        Directories.DataDirectory[] src = initialDirectories;
        return Arrays.copyOf(src, src.length);
    }

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private static final ExecutorService flushExecutor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                                                                          StageManager.KEEPALIVE,
                                                                                          TimeUnit.SECONDS,
                                                                                          new LinkedBlockingQueue<Runnable>(),
                                                                                          new NamedThreadFactory("MemtableFlushWriter"),
                                                                                          "internal");

    // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
    private static final ExecutorService postFlushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                              StageManager.KEEPALIVE,
                                                                                              TimeUnit.SECONDS,
                                                                                              new LinkedBlockingQueue<Runnable>(),
                                                                                              new NamedThreadFactory("MemtablePostFlush"),
                                                                                              "internal");

    private static final ExecutorService reclaimExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                            StageManager.KEEPALIVE,
                                                                                            TimeUnit.SECONDS,
                                                                                            new LinkedBlockingQueue<Runnable>(),
                                                                                            new NamedThreadFactory("MemtableReclaimMemory"),
                                                                                            "internal");

    private static final String[] COUNTER_NAMES = new String[]{"raw", "count", "error", "string"};
    private static final String[] COUNTER_DESCS = new String[]
    { "partition key in raw hex bytes",
      "value of this partition for given sampler",
      "value is within the error bounds plus or minus of this",
      "the partition key turned into a human readable format" };
    private static final CompositeType COUNTER_COMPOSITE_TYPE;
    private static final TabularType COUNTER_TYPE;

    private static final String[] SAMPLER_NAMES = new String[]{"cardinality", "partitions"};
    private static final String[] SAMPLER_DESCS = new String[]
    { "cardinality of partitions",
      "list of counter results" };

    private static final String SAMPLING_RESULTS_NAME = "SAMPLING_RESULTS";
    private static final CompositeType SAMPLING_RESULT;

    public static final String SNAPSHOT_TRUNCATE_PREFIX = "truncated";
    public static final String SNAPSHOT_DROP_PREFIX = "dropped";

    static
    {
        try
        {
            OpenType<?>[] counterTypes = new OpenType[] { SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING };
            COUNTER_COMPOSITE_TYPE = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_NAMES, COUNTER_DESCS, counterTypes);
            COUNTER_TYPE = new TabularType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_COMPOSITE_TYPE, COUNTER_NAMES);

            OpenType<?>[] samplerTypes = new OpenType[] { SimpleType.LONG, COUNTER_TYPE };
            SAMPLING_RESULT = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, SAMPLER_NAMES, SAMPLER_DESCS, samplerTypes);
        } catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public static final Comparator<Pair<Range<Token>, Integer>> lastRepairTimeComparator = new Comparator<Pair<Range<Token>, Integer>>()
    {
        public int compare(Pair<Range<Token>, Integer> pair1, Pair<Range<Token>, Integer> pair2)
        {
            if (pair1.right.equals(pair2.right))
            {
                return pair1.left.compareTo(pair2.left);
            }
            else
            {
                return pair2.right.compareTo(pair1.right);
            }
        }
    };

    public final Keyspace keyspace;
    public final String name;
    public final CFMetaData metadata;
    private final String mbeanName;
    @Deprecated
    private final String oldMBeanName;
    private volatile boolean valid = true;

    /**
     * Memtables and SSTables on disk for this column family.
     *
     * We synchronize on the Tracker to ensure isolation when we want to make sure
     * that the memtable we're acting on doesn't change out from under us.  I.e., flush
     * syncronizes on it to make sure it can submit on both executors atomically,
     * so anyone else who wants to make sure flush doesn't interfere should as well.
     */
    private final Tracker data;

    /* The read order, used to track accesses to off-heap memtable storage */
    public final OpOrder readOrdering = new OpOrder();

    /* This is used to generate the next index for a SSTable */
    private final AtomicInteger fileIndexGenerator = new AtomicInteger(0);

    public final SecondaryIndexManager indexManager;
    public final TableViews viewManager;

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultValue<Integer> minCompactionThreshold;
    private volatile DefaultValue<Integer> maxCompactionThreshold;
    private volatile DefaultValue<Double> crcCheckChance;

    private final CompactionStrategyManager compactionStrategyManager;

    private volatile Directories directories;

    public final TableMetrics metric;
    public volatile long sampleLatencyNanos;

    private final SSTableImporter sstableImporter;

    private volatile boolean compactionSpaceCheck = true;

    private volatile boolean neverPurgeTombstones = false;
    /**
     * A list to store ranges and the time they were last repaired. The list is
     * reverse-sorted based on the timestamp (see {@link ColumnFamilyStore#lastRepairTimeComparator}).
     */
    private final AtomicReference<SuccessfulRepairTimeHolder> lastSuccessfulRepair = new AtomicReference<>(SuccessfulRepairTimeHolder.EMPTY);

    public static void shutdownPostFlushExecutor() throws InterruptedException
    {
        postFlushExecutor.shutdown();
        postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);
    }

    public void reload()
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultValue(metadata.params.compaction.minCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultValue(metadata.params.compaction.maxCompactionThreshold());
        if (!crcCheckChance.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.crcCheckChance = new DefaultValue(metadata.params.crcCheckChance);

        compactionStrategyManager.maybeReload(metadata);
        directories = compactionStrategyManager.getDirectories();

        scheduleFlush();

        indexManager.reload();

        // If the CF comparator has changed, we need to change the memtable,
        // because the old one still aliases the previous comparator.
        if (data.getView().getCurrentMemtable().initialComparator != metadata.comparator)
            switchMemtable();
    }

    void scheduleFlush()
    {
        int period = metadata.params.memtableFlushPeriodInMs;
        if (period > 0)
        {
            logger.trace("scheduling flush in {} ms", period);
            WrappedRunnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws Exception
                {
                    synchronized (data)
                    {
                        Memtable current = data.getView().getCurrentMemtable();
                        // if we're not expired, we've been hit by a scheduled flush for an already flushed memtable, so ignore
                        if (current.isExpired())
                        {
                            if (current.isClean())
                            {
                                // if we're still clean, instead of swapping just reschedule a flush for later
                                scheduleFlush();
                            }
                            else
                            {
                                // we'll be rescheduled by the constructor of the Memtable.
                                forceFlush();
                            }
                        }
                    }
                }
            };
            ScheduledExecutors.scheduledTasks.schedule(runnable, period, TimeUnit.MILLISECONDS);
        }
    }

    public static Runnable getBackgroundCompactionTaskSubmitter()
    {
        return new Runnable()
        {
            public void run()
            {
                for (Keyspace keyspace : Keyspace.all())
                    for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                        CompactionManager.instance.submitBackground(cfs);
            }
        };
    }

    public void setCompactionParametersJson(String options)
    {
        setCompactionParameters(FBUtilities.fromJsonMap(options));
    }

    public String getCompactionParametersJson()
    {
        return FBUtilities.json(getCompactionParameters());
    }

    public void setCompactionParameters(Map<String, String> options)
    {
        try
        {
            CompactionParams compactionParams = CompactionParams.fromMap(options);
            compactionParams.validate();
            compactionStrategyManager.setNewLocalCompactionStrategy(compactionParams);
        }
        catch (Throwable t)
        {
            logger.error("Could not set new local compaction strategy", t);
            // dont propagate the ConfigurationException over jmx, user will only see a ClassNotFoundException
            throw new IllegalArgumentException("Could not set new local compaction strategy: "+t.getMessage());
        }
    }

    public Map<String, String> getCompactionParameters()
    {
        return compactionStrategyManager.getCompactionParams().asMap();
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata.params.compression.asMap();
    }

    public void setCompressionParameters(Map<String,String> opts)
    {
        try
        {
            metadata.compression(CompressionParams.fromMap(opts));
            metadata.params.compression.validate();
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private ColumnFamilyStore(Keyspace keyspace,
                             String columnFamilyName,
                             int generation,
                             CFMetaData metadata,
                             Directories directories,
                             boolean loadSSTables)
    {
        this(keyspace, columnFamilyName, generation, metadata, directories, loadSSTables, true);
    }


    @VisibleForTesting
    public ColumnFamilyStore(Keyspace keyspace,
                              String columnFamilyName,
                              int generation,
                              CFMetaData metadata,
                              Directories directories,
                              boolean loadSSTables,
                              boolean registerBookkeeping)
    {
        assert directories != null;
        assert metadata != null : "null metadata for " + keyspace + ":" + columnFamilyName;

        this.keyspace = keyspace;
        this.metadata = metadata;
        name = columnFamilyName;
        minCompactionThreshold = new DefaultValue<>(metadata.params.compaction.minCompactionThreshold());
        maxCompactionThreshold = new DefaultValue<>(metadata.params.compaction.maxCompactionThreshold());
        crcCheckChance = new DefaultValue<>(metadata.params.crcCheckChance);
        indexManager = new SecondaryIndexManager(this);
        viewManager = keyspace.viewManager.forTable(metadata);
        metric = new TableMetrics(this);
        fileIndexGenerator.set(generation);
        sampleLatencyNanos = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getReadRpcTimeout() / 2);

        logger.info("Initializing {}.{}", keyspace.getName(), name);

        // Create Memtable only on online
        Memtable initialMemtable = null;
        if (DatabaseDescriptor.isDaemonInitialized())
            initialMemtable = new Memtable(new AtomicReference<>(CommitLog.instance.getContext()), this);
        data = new Tracker(initialMemtable, loadSSTables);

        // scan for sstables corresponding to this cf and load them
        if (data.loadsstables)
        {
            Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
            Collection<SSTableReader> sstables = SSTableReader.openAll(sstableFiles.list().entrySet(), metadata);
            data.addInitialSSTables(sstables);
        }

        // compaction strategy should be created after the CFS has been prepared
        compactionStrategyManager = new CompactionStrategyManager(this);
        this.directories = compactionStrategyManager.getDirectories();

        if (maxCompactionThreshold.value() <= 0 || minCompactionThreshold.value() <=0)
        {
            logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
            this.compactionStrategyManager.disable();
        }

        // create the private ColumnFamilyStores for the secondary column indexes
        for (IndexMetadata info : metadata.getIndexes())
            indexManager.addIndex(info);

        if (registerBookkeeping)
        {
            // register the mbean
            mbeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s",
                                         isIndex() ? "IndexTables" : "Tables",
                                         keyspace.getName(), name);
            oldMBeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,columnfamily=%s",
                                         isIndex() ? "IndexColumnFamilies" : "ColumnFamilies",
                                         keyspace.getName(), name);
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
                for (ObjectName objectName : objectNames)
                {
                    mbs.registerMBean(this, objectName);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            mbeanName = null;
            oldMBeanName= null;
        }
        sstableImporter = new SSTableImporter(this);
    }

    public void updateSpeculationThreshold()
    {
        try
        {
            sampleLatencyNanos = metadata.params.speculativeRetry.calculateThreshold(metric.coordinatorReadLatency);
        }
        catch (Throwable e)
        {
            logger.error("Exception caught while calculating speculative retry threshold for {}.{}: {}", metadata.ksName, metadata.cfName, e);
        }
    }

    public Directories getDirectories()
    {
        return directories;
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        MetadataCollector collector = new MetadataCollector(metadata.comparator).sstableLevel(sstableLevel);
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, collector, header, lifecycleNewTracker);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector metadataCollector, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        return getCompactionStrategyManager().createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, metadataCollector, header, lifecycleNewTracker);
    }

    public boolean supportsEarlyOpen()
    {
        return compactionStrategyManager.supportsEarlyOpen();
    }

    /** call when dropping or renaming a CF. Performs mbean housekeeping and invalidates CFS to other operations */
    public void invalidate()
    {
        invalidate(true);
    }

    public void invalidate(boolean expectMBean)
    {
        // disable and cancel in-progress compactions before invalidating
        valid = false;

        try
        {
            unregisterMBean();
        }
        catch (Exception e)
        {
            if (expectMBean)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // this shouldn't block anything.
                logger.warn("Failed unregistering mbean: {}", mbeanName, e);
            }
        }

        compactionStrategyManager.shutdown();
        SystemKeyspace.removeTruncationRecord(metadata.cfId);

        data.dropSSTables();
        LifecycleTransaction.waitForDeletions();
        indexManager.invalidateAllIndexesBlocking();

        invalidateCaches();
    }

    /**
     * Removes every SSTable in the directory from the Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void maybeRemoveUnreadableSSTables(File directory)
    {
        data.removeUnreadableSSTables(directory);
    }

    void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
        for (ObjectName objectName : objectNames)
        {
            if (mbs.isRegistered(objectName))
                mbs.unregisterMBean(objectName);
        }

        // unregister metrics
        metric.release();
    }


    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, CFMetaData metadata, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, metadata.cfName, metadata, loadSSTables);
    }

    public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         CFMetaData metadata,
                                                                         boolean loadSSTables)
    {
        // get the max generation number, to prevent generation conflicts
        Directories directories = new Directories(metadata, initialDirectories);
        Directories.SSTableLister lister = directories.sstableLister(Directories.OnTxnErr.IGNORE).includeBackups(true);
        List<Integer> generations = new ArrayList<Integer>();
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor desc = entry.getKey();
            generations.add(desc.generation);
            if (!desc.isCompatible())
                throw new RuntimeException(String.format("Incompatible SSTable found. Current version %s is unable to read file: %s. Please run upgradesstables.",
                        desc.getFormat().getLatestVersion(), desc));
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;
        return new ColumnFamilyStore(keyspace, columnFamily, value, metadata, directories, loadSSTables);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void scrubDataDirectories(CFMetaData metadata) throws StartupException
    {
        Directories directories = new Directories(metadata, initialDirectories);
        Set<File> cleanedDirectories = new HashSet<>();

         // clear ephemeral snapshots that were not properly cleared last session (CASSANDRA-7357)
        clearEphemeralSnapshots(directories);

        directories.removeTemporaryDirectories();

        logger.trace("Removing temporary or obsoleted files from unfinished operations for table {}", metadata.cfName);
        if (!LifecycleTransaction.removeUnfinishedLeftovers(metadata))
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot remove temporary or obsoleted files for %s.%s due to a problem with transaction " +
                                                     "log files. Please check records with problems in the log messages above and fix them. " +
                                                     "Refer to the 3.0 upgrading instructions in NEWS.txt " +
                                                     "for a description of transaction log files.", metadata.ksName, metadata.cfName));

        logger.trace("Further extra check for orphan sstable files for {}", metadata.cfName);
        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister(Directories.OnTxnErr.IGNORE).list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            File directory = desc.directory;
            Set<Component> components = sstableFiles.getValue();

            if (!cleanedDirectories.contains(directory))
            {
                cleanedDirectories.add(directory);
                for (File tmpFile : desc.getTemporaryFiles())
                {
                    logger.info("Removing unfinished temporary file {}", tmpFile);
                    tmpFile.delete();
                }
            }

            File dataFile = new File(desc.filenameFor(Component.DATA));
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
                File file = new File(desc.filenameFor(component));
                if (file.exists())
                    FileUtils.deleteWithConfirm(desc.filenameFor(component));
            }
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(metadata.ksName + "-" + metadata.cfName + "-(Key|Row)Cache.*\\.tmp$");
        File dir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (dir.exists())
        {
            assert dir.isDirectory();
            for (File file : dir.listFiles())
                if (tmpCacheFilePattern.matcher(file.getName()).matches())
                    if (!file.delete())
                        logger.warn("could not delete {}", file.getAbsolutePath());
        }

        // also clean out any index leftovers.
        for (IndexMetadata index : metadata.getIndexes())
        {
            if (!index.isCustom())
            {
                CFMetaData indexMetadata = CassandraIndex.indexCfsMetadata(metadata, index);
                scrubDataDirectories(indexMetadata);
            }
        }
    }

    /**
     * Prior to 3.0, replacing compacted sstables was atomic as far as observers of DataTracker are concerned, but not on the
     * filesystem: first the new sstables are renamed to "live" status (i.e., the tmp marker is removed), then
     * their ancestors are removed.
     *
     * If an unclean shutdown for upgrade happens at the right time, we can thus end up with both the new ones and their
     * ancestors "live" in the system.  The most dangerous scenario here is that data from sstable A and a tombstone from sstable
     * B are compacted to sstable C (purging the tombstone and data), then sstable B is removed. A and C remain, and the data
     * is ressurected.
     *
     * To prevent this, we can use the record of sstables being compacted in the system keyspace and the ancestor metadata
     * to try to identify incomplete transactions.
     */
    public static void removeUnfinishedLegacyCompactionLeftovers(CFMetaData metadata, Map<Integer, UUID> unfinishedCompactions)
    {
        Directories directories = new Directories(metadata);
        Set<Integer> allGenerations = new HashSet<>();
        Map<Descriptor, Set<Component>> allSStables = directories.sstableLister(Directories.OnTxnErr.IGNORE).list();

        for (Descriptor desc : allSStables.keySet())
            allGenerations.add(desc.generation);

        // sanity-check unfinishedCompactions
        Set<Integer> unfinishedGenerations = unfinishedCompactions.keySet();
        if (!allGenerations.containsAll(unfinishedGenerations))
        {
            HashSet<Integer> missingGenerations = new HashSet<>(unfinishedGenerations);
            missingGenerations.removeAll(allGenerations);
            logger.info("Unfinished compactions of {}.{} reference missing sstables of generations {}",
                    metadata.ksName, metadata.cfName, missingGenerations);
        }

        // remove new sstables from compactions that didn't complete, and compute
        // set of ancestors that shouldn't exist anymore
        Set<Integer> completedAncestors = new HashSet<>();
        for (Map.Entry<Descriptor, Set<Component>> sstableFiles : directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true).list().entrySet())
        {
            // we rename the Data component last - if it does not exist as a final file, we should ignore this sstable and
            // it will be removed during startup
            if (!sstableFiles.getValue().contains(Component.DATA))
                continue;

            Descriptor desc = sstableFiles.getKey();
            if(!desc.version.hasCompactionAncestors())
                continue;

            Set<Integer> ancestors;
            try
            {
                CompactionMetadata compactionMetadata = (CompactionMetadata) desc.getMetadataSerializer().deserialize(desc, MetadataType.COMPACTION);
                ancestors = compactionMetadata.ancestors;
            }
            catch (IOException e)
            {
                throw new FSReadError(e, desc.filenameFor(Component.STATS));
            }
            catch (NullPointerException e)
            {
                throw new FSReadError(e, "Failed to remove unfinished compaction leftovers (file: " + desc.filenameFor(Component.STATS) + ").  See log for details.");
            }

            if (!ancestors.isEmpty()
                    && unfinishedGenerations.containsAll(ancestors)
                    && allGenerations.containsAll(ancestors))
            {
                // any of the ancestors would work, so we'll just lookup the compaction task ID with the first one
                UUID compactionTaskID = unfinishedCompactions.get(ancestors.iterator().next());
                assert compactionTaskID != null;
                logger.info("Going to delete unfinished compaction product {}", desc);
                SSTable.delete(desc, sstableFiles.getValue());
            }
            else
            {
                completedAncestors.addAll(ancestors);
            }
        }

        // remove old sstables from compactions that did complete
        for (Map.Entry<Descriptor, Set<Component>> sstableFiles : allSStables.entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            if(!desc.version.hasCompactionAncestors())
                continue;

            if (completedAncestors.contains(desc.generation) && !unfinishedGenerations.contains(desc.generation))
            {
                // if any of the ancestors were participating in a compaction, finish that compaction
                logger.info("Going to delete leftover compaction ancestor {}", desc);
                SSTable.delete(desc, sstableFiles.getValue());
            }
            else if (completedAncestors.contains(desc.generation))
            {
                logger.info("Generation {} was an input in an unfinished compaction, can't delete the file", desc.generation);
            }
        }
    }

    /**
     * See #{@code StorageService.importNewSSTables} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    @Deprecated
    public static void loadNewSSTables(String ksName, String cfName, String dirPath)
    {
        /** ks/cf existence checks will be done by open and getCFS methods for us */
        Keyspace keyspace = Keyspace.open(ksName);
        keyspace.getColumnFamilyStore(cfName).loadNewSSTables(dirPath);
    }

    @Deprecated
    public synchronized void loadNewSSTables(String dirPath)
    {
        SSTableImporter.Options options = SSTableImporter.Options.options(dirPath)
                                                                .resetLevel(true)
                                                                .verifySSTables(true)
                                                                .verifyTokens(true).build();
        sstableImporter.importNewSSTables(options);
    }

    @Deprecated
    public void loadNewSSTables()
    {
        SSTableImporter.Options options = SSTableImporter.Options.options().resetLevel(true).build();
        sstableImporter.importNewSSTables(options);
    }

    /**
     * #{@inheritDoc}
     */
    public synchronized List<String> importNewSSTables(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean extendedVerify)
    {
        SSTableImporter.Options options = SSTableImporter.Options.options(srcPaths)
                                                                 .resetLevel(resetLevel)
                                                                 .clearRepaired(clearRepaired)
                                                                 .verifySSTables(verifySSTables)
                                                                 .verifyTokens(verifyTokens)
                                                                 .invalidateCaches(invalidateCaches)
                                                                 .extendedVerify(extendedVerify).build();
        return sstableImporter.importNewSSTables(options);
    }

    Descriptor getUniqueDescriptorFor(Descriptor descriptor, File targetDirectory)
    {
        Descriptor newDescriptor;
        do
        {
            newDescriptor = new Descriptor(descriptor.version,
                                           targetDirectory,
                                           descriptor.ksname,
                                           descriptor.cfname,
                                           // Increment the generation until we find a filename that doesn't exist. This is needed because the new
                                           // SSTables that are being loaded might already use these generation numbers.
                                           fileIndexGenerator.incrementAndGet(),
                                           descriptor.formatType,
                                           descriptor.digestComponent);
        }
        while (new File(newDescriptor.filenameFor(Component.DATA)).exists());
        return newDescriptor;
    }

    public void rebuildSecondaryIndex(String idxName)
    {
        rebuildSecondaryIndex(keyspace.getName(), metadata.cfName, idxName);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

        Set<String> indexes = new HashSet<String>(Arrays.asList(idxNames));

        Iterable<SSTableReader> sstables = cfs.getSSTables(SSTableSet.CANONICAL);
        try (Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            logger.info("User Requested secondary index re-build for {}/{} indexes: {}", ksName, cfName, Joiner.on(',').join(idxNames));
            cfs.indexManager.rebuildIndexesBlocking(refs, indexes);
        }
    }

    @Deprecated
    public String getColumnFamilyName()
    {
        return getTableName();
    }

    public String getTableName()
    {
        return name;
    }

    public String getSSTablePath(File directory)
    {
        return getSSTablePath(directory, DatabaseDescriptor.getSSTableFormat().info.getLatestVersion(), DatabaseDescriptor.getSSTableFormat());
    }

    public String getSSTablePath(File directory, SSTableFormat.Type format)
    {
        return getSSTablePath(directory, format.info.getLatestVersion(), format);
    }

    private String getSSTablePath(File directory, Version version, SSTableFormat.Type format)
    {
        Descriptor desc = new Descriptor(version,
                                         directory,
                                         keyspace.getName(),
                                         name,
                                         fileIndexGenerator.incrementAndGet(),
                                         format,
                                         Component.digestFor(BigFormat.latestVersion.uncompressedChecksumType()));
        return desc.filenameFor(Component.DATA);
    }

    /**
     * Switches the memtable iff the live memtable is the one provided
     *
     * @param memtable
     */
    public ListenableFuture<ReplayPosition> switchMemtableIfCurrent(Memtable memtable)
    {
        synchronized (data)
        {
            if (data.getView().getCurrentMemtable() == memtable)
                return switchMemtable();
        }
        return waitForFlushes();
    }

    /*
     * switchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables where it is available for reads.
     * This method does not block except for synchronizing on Tracker, but the Future it returns will
     * not complete until the Memtable (and all prior Memtables) have been successfully flushed, and the CL
     * marked clean up to the position owned by the Memtable.
     */
    public ListenableFuture<ReplayPosition> switchMemtable()
    {
        synchronized (data)
        {
            logFlush();
            Flush flush = new Flush(false);
            flushExecutor.execute(flush);
            ListenableFutureTask<ReplayPosition> task = ListenableFutureTask.create(flush.postFlush);
            postFlushExecutor.execute(task);
            return task;
        }
    }

    // print out size of all memtables we're enqueuing
    private void logFlush()
    {
        // reclaiming includes that which we are GC-ing;
        float onHeapRatio = 0, offHeapRatio = 0;
        long onHeapTotal = 0, offHeapTotal = 0;
        Memtable memtable = getTracker().getView().getCurrentMemtable();
        onHeapRatio +=  memtable.getAllocator().onHeap().ownershipRatio();
        offHeapRatio += memtable.getAllocator().offHeap().ownershipRatio();
        onHeapTotal += memtable.getAllocator().onHeap().owns();
        offHeapTotal += memtable.getAllocator().offHeap().owns();

        for (ColumnFamilyStore indexCfs : indexManager.getAllIndexColumnFamilyStores())
        {
            MemtableAllocator allocator = indexCfs.getTracker().getView().getCurrentMemtable().getAllocator();
            onHeapRatio += allocator.onHeap().ownershipRatio();
            offHeapRatio += allocator.offHeap().ownershipRatio();
            onHeapTotal += allocator.onHeap().owns();
            offHeapTotal += allocator.offHeap().owns();
        }

        logger.info("Enqueuing flush of {}: {}", name, String.format("%d (%.0f%%) on-heap, %d (%.0f%%) off-heap",
                                                                     onHeapTotal, onHeapRatio * 100, offHeapTotal, offHeapRatio * 100));
    }


    /**
     * Flush if there is unflushed data in the memtables
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public ListenableFuture<ReplayPosition> forceFlush()
    {
        synchronized (data)
        {
            Memtable current = data.getView().getCurrentMemtable();
            for (ColumnFamilyStore cfs : concatWithIndexes())
                if (!cfs.data.getView().getCurrentMemtable().isClean())
                    return switchMemtableIfCurrent(current);
            return waitForFlushes();
        }
    }

    /**
     * Flush if there is unflushed data that was written to the CommitLog before @param flushIfDirtyBefore
     * (inclusive).
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public ListenableFuture<ReplayPosition> forceFlush(ReplayPosition flushIfDirtyBefore)
    {
        // we don't loop through the remaining memtables since here we only care about commit log dirtiness
        // and this does not vary between a table and its table-backed indexes
        Memtable current = data.getView().getCurrentMemtable();
        if (current.mayContainDataBefore(flushIfDirtyBefore))
            return switchMemtableIfCurrent(current);
        return waitForFlushes();
    }

    /**
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    private ListenableFuture<ReplayPosition> waitForFlushes()
    {
        // we grab the current memtable; once any preceding memtables have flushed, we know its
        // commitLogLowerBound has been set (as this it is set with the upper bound of the preceding memtable)
        final Memtable current = data.getView().getCurrentMemtable();
        ListenableFutureTask<ReplayPosition> task = ListenableFutureTask.create(new Callable<ReplayPosition>()
        {
            public ReplayPosition call()
            {
                logger.debug("forceFlush requested but everything is clean in {}", name);
                return current.getCommitLogLowerBound();
            }
        });
        postFlushExecutor.execute(task);
        return task;
    }

    public ReplayPosition forceBlockingFlush()
    {
        return FBUtilities.waitOnFuture(forceFlush());
    }

    /**
     * Both synchronises custom secondary indexes and provides ordering guarantees for futures on switchMemtable/flush
     * etc, which expect to be able to wait until the flush (and all prior flushes) requested have completed.
     */
    private final class PostFlush implements Callable<ReplayPosition>
    {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile Throwable flushFailure = null;
        final List<Memtable> memtables;

        private PostFlush(List<Memtable> memtables)
        {
            this.memtables = memtables;
        }

        public ReplayPosition call()
        {
            try
            {
                // we wait on the latch for the commitLogUpperBound to be set, and so that waiters
                // on this task can rely on all prior flushes being complete
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException();
            }

            ReplayPosition commitLogUpperBound = ReplayPosition.NONE;
            // If a flush errored out but the error was ignored, make sure we don't discard the commit log.
            if (flushFailure == null && !memtables.isEmpty())
            {
                Memtable memtable = memtables.get(0);
                commitLogUpperBound = memtable.getCommitLogUpperBound();
                CommitLog.instance.discardCompletedSegments(metadata.cfId, memtable.getCommitLogLowerBound(), commitLogUpperBound);
            }

            metric.pendingFlushes.dec();

            if (flushFailure != null)
                Throwables.propagate(flushFailure);

            return commitLogUpperBound;
        }
    }

    /**
     * Should only be constructed/used from switchMemtable() or truncate(), with ownership of the Tracker monitor.
     * In the constructor the current memtable(s) are swapped, and a barrier on outstanding writes is issued;
     * when run by the flushWriter the barrier is waited on to ensure all outstanding writes have completed
     * before all memtables are immediately written, and the CL is either immediately marked clean or, if
     * there are custom secondary indexes, the post flush clean up is left to update those indexes and mark
     * the CL clean
     */
    private final class Flush implements Runnable
    {
        final OpOrder.Barrier writeBarrier;
        final List<Memtable> memtables = new ArrayList<>();
        final PostFlush postFlush;
        final boolean truncate;

        private Flush(boolean truncate)
        {
            // if true, we won't flush, we'll just wait for any outstanding writes, switch the memtable, and discard
            this.truncate = truncate;

            metric.pendingFlushes.inc();
            /**
             * To ensure correctness of switch without blocking writes, run() needs to wait for all write operations
             * started prior to the switch to complete. We do this by creating a Barrier on the writeOrdering
             * that all write operations register themselves with, and assigning this barrier to the memtables,
             * after which we *.issue()* the barrier. This barrier is used to direct write operations started prior
             * to the barrier.issue() into the memtable we have switched out, and any started after to its replacement.
             * In doing so it also tells the write operations to update the commitLogUpperBound of the memtable, so
             * that we know the CL position we are dirty to, which can be marked clean when we complete.
             */
            writeBarrier = keyspace.writeOrder.newBarrier();

            // submit flushes for the memtable for any indexed sub-cfses, and our own
            AtomicReference<ReplayPosition> commitLogUpperBound = new AtomicReference<>();
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                // switch all memtables, regardless of their dirty status, setting the barrier
                // so that we can reach a coordinated decision about cleanliness once they
                // are no longer possible to be modified
                Memtable newMemtable = new Memtable(commitLogUpperBound, cfs);
                Memtable oldMemtable = cfs.data.switchMemtable(truncate, newMemtable);
                oldMemtable.setDiscarding(writeBarrier, commitLogUpperBound);
                memtables.add(oldMemtable);
            }

            // we then ensure an atomic decision is made about the upper bound of the continuous range of commit log
            // records owned by this memtable
            setCommitLogUpperBound(commitLogUpperBound);

            // we then issue the barrier; this lets us wait for all operations started prior to the barrier to complete;
            // since this happens after wiring up the commitLogUpperBound, we also know all operations with earlier
            // replay positions have also completed, i.e. the memtables are done and ready to flush
            writeBarrier.issue();
            postFlush = new PostFlush(memtables);
        }

        public void run()
        {
            // mark writes older than the barrier as blocking progress, permitting them to exceed our memory limit
            // if they are stuck waiting on it, then wait for them all to complete
            writeBarrier.markBlocking();
            writeBarrier.await();

            // mark all memtables as flushing, removing them from the live memtable list
            for (Memtable memtable : memtables)
                memtable.cfs.data.markFlushing(memtable);

            metric.memtableSwitchCount.inc();

            try
            {
                boolean flushNonCf2i = true;
                for (Memtable memtable : memtables)
                {
                    Collection<SSTableReader> readers = Collections.emptyList();
                    if (!memtable.isClean() && !truncate)
                    {
                        // TODO: SecondaryIndex should support setBarrier(), so custom implementations can co-ordinate exactly
                        // with CL as we do with memtables/CFS-backed SecondaryIndexes.
                        if (flushNonCf2i)
                        {
                            indexManager.flushAllNonCFSBackedIndexesBlocking();
                            flushNonCf2i = false;
                        }
                        readers = memtable.flush();
                    }
                    memtable.cfs.replaceFlushed(memtable, readers);
                    reclaim(memtable);
                }
            }
            catch (Throwable e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // If we weren't killed, try to continue work but do not allow CommitLog to be discarded.
                postFlush.flushFailure = e;
            }
            finally
            {
                // signal the post-flush we've done our work
                postFlush.latch.countDown();
            }
        }

        private void reclaim(final Memtable memtable)
        {
            // issue a read barrier for reclaiming the memory, and offload the wait to another thread
            final OpOrder.Barrier readBarrier = readOrdering.newBarrier();
            readBarrier.issue();
            reclaimExecutor.execute(new WrappedRunnable()
            {
                public void runMayThrow() throws InterruptedException, ExecutionException
                {
                    readBarrier.await();
                    memtable.setDiscarded();
                }
            });
        }
    }

    // atomically set the upper bound for the commit log
    private static void setCommitLogUpperBound(AtomicReference<ReplayPosition> commitLogUpperBound)
    {
        // we attempt to set the holder to the current commit log context. at the same time all writes to the memtables are
        // also maintaining this value, so if somebody sneaks ahead of us somehow (should be rare) we simply retry,
        // so that we know all operations prior to the position have not reached it yet
        ReplayPosition lastReplayPosition;
        while (true)
        {
            lastReplayPosition = new Memtable.LastReplayPosition(CommitLog.instance.getContext());
            ReplayPosition currentLast = commitLogUpperBound.get();
            if ((currentLast == null || currentLast.compareTo(lastReplayPosition) <= 0)
                && commitLogUpperBound.compareAndSet(currentLast, lastReplayPosition))
                break;
        }
    }

    /**
     * Finds the largest memtable, as a percentage of *either* on- or off-heap memory limits, and immediately
     * queues it for flushing. If the memtable selected is flushed before this completes, no work is done.
     */
    public static class FlushLargestColumnFamily implements Runnable
    {
        public void run()
        {
            float largestRatio = 0f;
            Memtable largest = null;
            float liveOnHeap = 0, liveOffHeap = 0;
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                // we take a reference to the current main memtable for the CF prior to snapping its ownership ratios
                // to ensure we have some ordering guarantee for performing the switchMemtableIf(), i.e. we will only
                // swap if the memtables we are measuring here haven't already been swapped by the time we try to swap them
                Memtable current = cfs.getTracker().getView().getCurrentMemtable();

                // find the total ownership ratio for the memtable and all SecondaryIndexes owned by this CF,
                // both on- and off-heap, and select the largest of the two ratios to weight this CF
                float onHeap = 0f, offHeap = 0f;
                onHeap += current.getAllocator().onHeap().ownershipRatio();
                offHeap += current.getAllocator().offHeap().ownershipRatio();

                for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
                {
                    MemtableAllocator allocator = indexCfs.getTracker().getView().getCurrentMemtable().getAllocator();
                    onHeap += allocator.onHeap().ownershipRatio();
                    offHeap += allocator.offHeap().ownershipRatio();
                }

                float ratio = Math.max(onHeap, offHeap);
                if (ratio > largestRatio)
                {
                    largest = current;
                    largestRatio = ratio;
                }

                liveOnHeap += onHeap;
                liveOffHeap += offHeap;
            }

            if (largest != null)
            {
                float usedOnHeap = Memtable.MEMORY_POOL.onHeap.usedRatio();
                float usedOffHeap = Memtable.MEMORY_POOL.offHeap.usedRatio();
                float flushingOnHeap = Memtable.MEMORY_POOL.onHeap.reclaimingRatio();
                float flushingOffHeap = Memtable.MEMORY_POOL.offHeap.reclaimingRatio();
                float thisOnHeap = largest.getAllocator().onHeap().ownershipRatio();
                float thisOffHeap = largest.getAllocator().offHeap().ownershipRatio();
                logger.info("Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}",
                            largest.cfs, ratio(usedOnHeap, usedOffHeap), ratio(liveOnHeap, liveOffHeap),
                            ratio(flushingOnHeap, flushingOffHeap), ratio(thisOnHeap, thisOffHeap));
                largest.cfs.switchMemtableIfCurrent(largest);
            }
        }
    }

    private static String ratio(float onHeap, float offHeap)
    {
        return String.format("%.2f/%.2f", onHeap, offHeap);
    }

    public void maybeUpdateRowCache(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return;

        RowCacheKey cacheKey = new RowCacheKey(metadata.ksAndCFName, key);
        invalidateCachedPartition(cacheKey);
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Keyspace.switchLock
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    public void apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, ReplayPosition replayPosition)

    {
        long start = System.nanoTime();
        Memtable mt = data.getMemtableFor(opGroup, replayPosition);
        try
        {
            long timeDelta = mt.put(update, indexer, opGroup);
            DecoratedKey key = update.partitionKey();
            maybeUpdateRowCache(key);
            metric.samplers.get(Sampler.WRITES).addSample(key.getKey(), key.hashCode(), 1);
            metric.writeLatency.addNano(System.nanoTime() - start);
            // CASSANDRA-11117 - certain resolution paths on memtable put can result in very
            // large time deltas, either through a variety of sentinel timestamps (used for empty values, ensuring
            // a minimal write, etc). This limits the time delta to the max value the histogram
            // can bucket correctly. This also filters the Long.MAX_VALUE case where there was no previous value
            // to update.
            if(timeDelta < Long.MAX_VALUE)
                metric.colUpdateTimeDeltaHistogram.update(Math.min(18165375903306L, timeDelta));
        }
        catch (RuntimeException e)
        {
            throw new RuntimeException(e.getMessage()
                                                     + " for ks: "
                                                     + keyspace.getName() + ", table: " + name, e);
        }

    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Collection<SSTableReader> getOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        logger.trace("Checking for sstables overlapping {}", sstables);

        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (!sstables.iterator().hasNext())
            return ImmutableSet.of();

        View view = data.getView();

        List<SSTableReader> sortedByFirst = Lists.newArrayList(sstables);
        Collections.sort(sortedByFirst, (o1, o2) -> o1.first.compareTo(o2.first));

        List<AbstractBounds<PartitionPosition>> bounds = new ArrayList<>();
        DecoratedKey first = null, last = null;
        /*
        normalize the intervals covered by the sstables
        assume we have sstables like this (brackets representing first/last key in the sstable);
        [   ] [   ]    [   ]   [  ]
           [   ]         [       ]
        then we can, instead of searching the interval tree 6 times, normalize the intervals and
        only query the tree 2 times, for these intervals;
        [         ]    [          ]
         */
        for (SSTableReader sstable : sortedByFirst)
        {
            if (first == null)
            {
                first = sstable.first;
                last = sstable.last;
            }
            else
            {
                if (sstable.first.compareTo(last) <= 0) // we do overlap
                {
                    if (sstable.last.compareTo(last) > 0)
                        last = sstable.last;
                }
                else
                {
                    bounds.add(AbstractBounds.bounds(first, true, last, true));
                    first = sstable.first;
                    last = sstable.last;
                }
            }
        }
        bounds.add(AbstractBounds.bounds(first, true, last, true));
        Set<SSTableReader> results = new HashSet<>();

        for (AbstractBounds<PartitionPosition> bound : bounds)
            Iterables.addAll(results, view.liveSSTablesInBounds(bound.left, bound.right));

        return Sets.difference(results, ImmutableSet.copyOf(sstables));
    }

    /**
     * like getOverlappingSSTables, but acquires references before returning
     */
    public Refs<SSTableReader> getAndReferenceOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        while (true)
        {
            Iterable<SSTableReader> overlapped = getOverlappingLiveSSTables(sstables);
            Refs<SSTableReader> refs = Refs.tryRef(overlapped);
            if (refs != null)
                return refs;
        }
    }

    /*
     * Called after a BinaryMemtable flushes its in-memory data, or we add a file
     * via bootstrap. This information is cached in the ColumnFamilyStore.
     * This is useful for reads because the ColumnFamilyStore first looks in
     * the in-memory store and the into the disk to find the key. If invoked
     * during recoveryMode the onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     */
    public void addSSTable(SSTableReader sstable)
    {
        assert sstable.getColumnFamilyName().equals(name);
        addSSTables(Arrays.asList(sstable));
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        data.addSSTables(sstables);
        CompactionManager.instance.submitBackground(this);
    }

    /**
     * Calculate expected file size of SSTable after compaction.
     *
     * If operation type is {@code CLEANUP} and we're not dealing with an index sstable,
     * then we calculate expected file size with checking token range to be eliminated.
     *
     * Otherwise, we just add up all the files' size, which is the worst case file
     * size for compaction of all the list of files given.
     *
     * @param sstables SSTables to calculate expected compacted file size
     * @param operation Operation type
     * @return Expected file size of SSTable after compaction
     */
    public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operation)
    {
        if (operation != OperationType.CLEANUP || isIndex())
        {
            return SSTableReader.getTotalBytes(sstables);
        }

        // cleanup size estimation only counts bytes for keys local to this node
        long expectedFileSize = 0;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(ranges);
            for (Pair<Long, Long> position : positions)
                expectedFileSize += position.right - position.left;
        }

        double compressionRatio = metric.compressionRatio.getValue();
        if (compressionRatio > 0d)
            expectedFileSize *= compressionRatio;

        return expectedFileSize;
    }

    /*
     *  Find the maximum size file in the list .
     */
    public SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.onDiskLength() > maxSize)
            {
                maxSize = sstable.onDiskLength();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    public CompactionManager.AllSSTableOpStatus forceCleanup(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performCleanup(ColumnFamilyStore.this, jobs);
    }

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs) throws ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, false, checkData, jobs);
    }

    @VisibleForTesting
    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean reinsertOverflowedTTL, boolean alwaysFail, boolean checkData, int jobs) throws ExecutionException, InterruptedException
    {
        // skip snapshot creation during scrub, SEE JIRA 5891
        if(!disableSnapshot)
            snapshotWithoutFlush("pre-scrub-" + System.currentTimeMillis());

        try
        {
            return CompactionManager.instance.performScrub(ColumnFamilyStore.this, skipCorrupted, checkData, reinsertOverflowedTTL, jobs);
        }
        catch(Throwable t)
        {
            if (!rebuildOnFailedScrub(t))
                throw t;

            return alwaysFail ? CompactionManager.AllSSTableOpStatus.ABORTED : CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        }
    }

    /**
     * CASSANDRA-5174 : For an index cfs we may be able to discard everything and just rebuild
     * the index when a scrub fails.
     *
     * @return true if we are an index cfs and we successfully rebuilt the index
     */
    public boolean rebuildOnFailedScrub(Throwable failure)
    {
        if (!isIndex() || !SecondaryIndexManager.isIndexColumnFamilyStore(this))
            return false;

        truncateBlocking();

        logger.warn("Rebuilding index for {} because of <{}>", name, failure.getMessage());

        ColumnFamilyStore parentCfs = SecondaryIndexManager.getParentCfs(this);
        assert parentCfs.indexManager.getAllIndexColumnFamilyStores().contains(this);

        String indexName = SecondaryIndexManager.getIndexName(this);

        parentCfs.rebuildSecondaryIndex(indexName);
        return true;
    }

    public CompactionManager.AllSSTableOpStatus verify(Verifier.Options options) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performVerify(ColumnFamilyStore.this, options);
    }

    public CompactionManager.AllSSTableOpStatus sstablesRewrite(boolean excludeCurrentVersion, int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this, excludeCurrentVersion, jobs);
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        assert !sstables.isEmpty();
        maybeFail(data.dropSSTables(Predicates.in(sstables), compactionType, null));
    }

    void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        compactionStrategyManager.replaceFlushed(memtable, sstables);
    }

    public boolean isValid()
    {
        return valid;
    }

    /**
     * Package protected for access from the CompactionManager.
     */
    public Tracker getTracker()
    {
        return data;
    }

    public Set<SSTableReader> getLiveSSTables()
    {
        return data.getView().liveSSTables();
    }

    public Iterable<SSTableReader> getSSTables(SSTableSet sstableSet)
    {
        return data.getView().select(sstableSet);
    }

    public Iterable<SSTableReader> getUncompactingSSTables()
    {
        return data.getUncompacting();
    }

    public boolean isFilterFullyCoveredBy(ClusteringIndexFilter filter, DataLimits limits, CachedPartition cached, int nowInSec)
    {
        // We can use the cached value only if we know that no data it doesn't contain could be covered
        // by the query filter, that is if:
        //   1) either the whole partition is cached
        //   2) or we can ensure than any data the filter selects is in the cached partition

        // We can guarantee that a partition is fully cached if the number of rows it contains is less than
        // what we're caching. Wen doing that, we should be careful about expiring cells: we should count
        // something expired that wasn't when the partition was cached, or we could decide that the whole
        // partition is cached when it's not. This is why we use CachedPartition#cachedLiveRows.
        if (cached.cachedLiveRows() < metadata.params.caching.rowsPerPartitionToCache())
            return true;

        // If the whole partition isn't cached, then we must guarantee that the filter cannot select data that
        // is not in the cache. We can guarantee that if either the filter is a "head filter" and the cached
        // partition has more live rows that queried (where live rows refers to the rows that are live now),
        // or if we can prove that everything the filter selects is in the cached partition based on its content.
        return (filter.isHeadFilter() && limits.hasEnoughLiveData(cached,
                                                                  nowInSec,
                                                                  filter.selectsAllPartition(),
                                                                  metadata.enforceStrictLiveness()))
                || filter.isFullyCoveredBy(cached);
    }

    private boolean skipRFCheckForXmasPatch = false;
    @VisibleForTesting
    public void skipRFCheckForXmasPatch()
    {
        this.skipRFCheckForXmasPatch = true;
    }

    private boolean useRepairHistory()
    {
        return skipRFCheckForXmasPatch || keyspace.getReplicationStrategy().getReplicationFactor() > 1;
    }


    public void loadLastSuccessfulRepair()
    {
        if (!useRepairHistory() || !DatabaseDescriptor.enableShadowChristmasPatch())
            return;
        setLastSuccessfulRepairs(SystemKeyspace.getLastSuccessfulRepair(keyspace.getName(), name),
                                 SystemKeyspace.getInvalidatedSuccessfulRepairRanges(keyspace.getName(), name));
    }

    /**
     * for testing ONLY
     */
    @VisibleForTesting
    public void clearRepairedRangeUnsafes()
    {
        clearLastSucessfulRepairUnsafe();
        SystemKeyspace.clearRepairedRanges(keyspace.getName(), getTableName());
    }

    @VisibleForTesting
    void setLastSuccessfulRepairs(Map<Range<Token>, Integer> storedSuccessfulRepairs, List<InvalidatedRepairedRange> storedInvalidatedRepairs)
    {
        synchronized (lastSuccessfulRepair)
        {
            SuccessfulRepairTimeHolder current = lastSuccessfulRepair.get();
            if (!current.successfulRepairs.isEmpty())
                throw new IllegalStateException("Last Successful repair should be empty. " + lastSuccessfulRepair);

            List<Pair<Range<Token>, Integer>> nextBuilder = new ArrayList<>(storedSuccessfulRepairs.size());

            for (Map.Entry<Range<Token>, Integer> entry : storedSuccessfulRepairs.entrySet())
                nextBuilder.add(Pair.create(entry.getKey(), entry.getValue()));
            Collections.sort(nextBuilder, lastRepairTimeComparator);

            List<InvalidatedRepairedRange> newInvalidatedRepairs = new ArrayList<>(storedInvalidatedRepairs);
            newInvalidatedRepairs.sort((a, b) -> Ints.compare(a.minLDTSeconds, b.minLDTSeconds));

            lastSuccessfulRepair.set(new SuccessfulRepairTimeHolder(ImmutableList.copyOf(nextBuilder), ImmutableList.copyOf(newInvalidatedRepairs)));
        }
    }

    /**
     * for testing ONLY
     */
    @VisibleForTesting
    public void clearLastSucessfulRepairUnsafe()
    {
        synchronized (lastSuccessfulRepair)
        {
            lastSuccessfulRepair.set(SuccessfulRepairTimeHolder.EMPTY);
        }
    }

    public void updateLastSuccessfulRepair(Range<Token> range, long succeedAt)
    {
        if (!useRepairHistory())
            return;

        if (!DatabaseDescriptor.enableShadowChristmasPatch())
            return;
        int gcableTime = (int) (succeedAt / 1000);
        synchronized (lastSuccessfulRepair)
        {
            SuccessfulRepairTimeHolder current = lastSuccessfulRepair.get();
            SuccessfulRepairTimeHolder next = updateLastSuccessfulRepair(range, gcableTime, current);
            if (null == next)
                return;
            lastSuccessfulRepair.set(next);
            SystemKeyspace.updateLastSuccessfulRepair(metadata.ksName, name, range, succeedAt);
        }
        logger.debug("lastSuccessfulRepair:" + lastSuccessfulRepair);
    }

    /**
     * remove duplicate range, or put the <range, gcableTime> pair in the right place
     *
     * @return Updated list with new value, else null if the time is in the past.
     */
    @VisibleForTesting
    public static SuccessfulRepairTimeHolder updateLastSuccessfulRepair(Range<Token> range, int gcableTime, SuccessfulRepairTimeHolder currentRepairs)
    {
        List<Pair<Range<Token>, Integer>> previousTimes = currentRepairs.successfulRepairs;
        Map<Range<Token>, Integer> rangeTimes = new HashMap<>(previousTimes.size() + 1);
        for (Pair<Range<Token>, Integer> p: previousTimes)
        {
            if (!rangeTimes.containsKey(p.left) || rangeTimes.get(p.left) < p.right)
            {
                rangeTimes.put(p.left, p.right);
            }
        }

        if (!rangeTimes.containsKey(range) || rangeTimes.get(range) < gcableTime)
        {
            rangeTimes.put(range, gcableTime);
        }
        else
        {
            logger.error(String.format("Last repair time is in the past. Ignoring. %s %s", range, gcableTime));
            return null;
        }

        List<Pair<Range<Token>, Integer>> nextTimes = new ArrayList<>(rangeTimes.size());
        for (Map.Entry<Range<Token>, Integer> entry: rangeTimes.entrySet())
        {
            nextTimes.add(Pair.create(entry.getKey(), entry.getValue()));
        }

        nextTimes.sort(lastRepairTimeComparator);
        return new SuccessfulRepairTimeHolder(ImmutableList.copyOf(nextTimes), currentRepairs.invalidatedRepairs);
    }


    /**
     * Basic idea is that we can't use the sstable boundaries straight up - we would have to subtract the
     * repaired ranges from all the invalidated ones after each successful repair, otherwise the table would
     * keep growing since we import sstables with arbitrary boundaries.
     *
     * So, instead we check which old successful repaired ranges the imported sstables intersect and insert
     * matching ranges in the invalidation table which we can then remove once we get a successful repair.
     * This assumes we don't change repair boundaries (-st, -et) every repair.
     *
     * If the invalidation table already contains an "active" invalidation (one where no repair has been run after
     * the invalidation/import was made) we make sure that the entry has the smallest ldt between the existing
     * one and the new one.
     */
    @VisibleForTesting
    void clearLastRepairTimesFor(Collection<SSTableReader> sstables, int nowSeconds)
    {
        if (!useRepairHistory() || !DatabaseDescriptor.enableShadowChristmasPatch())
            return;

        BoundsAndOldestTombstone bounds = new BoundsAndOldestTombstone(sstables);
        synchronized (lastSuccessfulRepair)
        {
            SuccessfulRepairTimeHolder current = lastSuccessfulRepair.get();

            Map<Range<Token>, InvalidatedRepairedRange> currentInvalidated = new HashMap<>();
            for (InvalidatedRepairedRange irr : current.invalidatedRepairs)
            {
                currentInvalidated.put(irr.range, irr);
            }

            for (Pair<Range<Token>, Integer> repair : current.successfulRepairs)
            {
                Range<Token> repairedRange = repair.left;
                int repairTime = repair.right;
                // find the smallest local deletion time among the intersecting sstables:
                int minLDT = bounds.getOldestIntersectingTombstoneLDT(repairedRange, repairTime);

                if (minLDT != Integer.MAX_VALUE)
                {
                    logger.info("Invalidating successfully repaired range {}", repairedRange);
                    InvalidatedRepairedRange newIRR = new InvalidatedRepairedRange(nowSeconds, minLDT, repairedRange);
                    if (currentInvalidated.containsKey(repairedRange))
                    {
                        InvalidatedRepairedRange mergedIRR = newIRR.merge(repairTime, currentInvalidated.get(repairedRange));
                        // this can be null if a repair is started and finished between us starting import and getting  here (ie, the
                        // repairTime > nowInSeconds) keeping the already inactive newIRR is safe though
                        if (mergedIRR != null)
                            newIRR = mergedIRR;
                    }
                    currentInvalidated.put(repairedRange, newIRR);
                    SystemKeyspace.invalidateSuccessfulRepair(keyspace.getName(), getTableName(), newIRR);
                }
            }
            List<InvalidatedRepairedRange> newInvalidatedRepairedRanges = new ArrayList<>(currentInvalidated.values());
            newInvalidatedRepairedRanges.sort((a, b) -> Ints.compare(a.minLDTSeconds, b.minLDTSeconds));
            lastSuccessfulRepair.set(new SuccessfulRepairTimeHolder(current.successfulRepairs, ImmutableList.copyOf(newInvalidatedRepairedRanges)));
        }
    }

    public SuccessfulRepairTimeHolder getRepairTimeSnapshot()
    {
        return lastSuccessfulRepair.get();
    }

    /**
     * Holds an immutable list of successfully repaired ranges, sorted by newest repair
     *
     * We 'snapshot' the list when we start compaction to make sure we don't drop any tombstones
     * that were marked repaired after the compaction started (because we might not include the newly streamed-in
     * sstables in the overlap calculation)
     */
    public static class SuccessfulRepairTimeHolder
    {
        @VisibleForTesting
        final ImmutableList<Pair<Range<Token>, Integer>> successfulRepairs;
        final ImmutableList<InvalidatedRepairedRange> invalidatedRepairs;

        public static final SuccessfulRepairTimeHolder EMPTY = new SuccessfulRepairTimeHolder(ImmutableList.of(), ImmutableList.of());

        public SuccessfulRepairTimeHolder(ImmutableList<Pair<Range<Token>, Integer>> successfulRepairs,
                                          ImmutableList<InvalidatedRepairedRange> invalidatedRepairs)
        {
            // we don't need to copy here - the list is never updated, it is swapped out
            this.successfulRepairs = successfulRepairs;
            this.invalidatedRepairs = invalidatedRepairs;
        }

        // todo: there is a possible compaction performance improvment here - we could sort the successfulRepairs by
        // range instead and binary search or use the fact that the tokens sent in to this method are increasing - we would
        // have to make sure the successfulRepairs are non-overlapping for this
        public int getLastSuccessfulRepairTimeFor(Token t)
        {
            assert t != null;
            int lastRepairTime = Integer.MIN_VALUE;
            // successfulRepairs are sorted by last repair time - this means that if we find a range that contains the
            // token, it is guaranteed to be the newest repair for that token
            for (Pair<Range<Token>, Integer> lastRepairSuccess : successfulRepairs)
            {
                if (lastRepairSuccess.left.contains(t))
                {
                    lastRepairTime = lastRepairSuccess.right;
                    break;
                }
            }
            if (lastRepairTime == Integer.MIN_VALUE)
                return lastRepairTime;

            // now we need to check if a range that contains this token has been invalidated by nodetool import:
            // invalidatedRepairs is sorted by smallest local deletion time first (ie, we keep the oldest tombstone ldt
            // first in the list since this is what we are interested in here)
            for (InvalidatedRepairedRange irr : invalidatedRepairs)
            {
                // if the nodetool import was run *after* the last repair time and the range repaired contains
                // the token, it is invalid and we must use the min ldt as a last repair time.
                if (irr.invalidatedAtSeconds > lastRepairTime && irr.range.contains(t))
                {
                    return irr.minLDTSeconds;
                }
            }
            return lastRepairTime;
        }

        public int gcBeforeForKey(ColumnFamilyStore cfs, DecoratedKey key, int fallbackGCBefore)
        {
            if (!DatabaseDescriptor.enableChristmasPatch() || !cfs.useRepairHistory())
                return fallbackGCBefore;

            return Math.min(getLastSuccessfulRepairTimeFor(key.getToken()), fallbackGCBefore);
        }

        public ImmutableList<Range<Token>> allRanges()
        {
            ImmutableList.Builder<Range<Token>> builder = ImmutableList.builder();
            for (Pair<Range<Token>, Integer> p : successfulRepairs)
                builder.add(p.left);
            return builder.build();
        }

        public SuccessfulRepairTimeHolder withoutInvalidationRepairs()
        {
            return new SuccessfulRepairTimeHolder(successfulRepairs, ImmutableList.of());
        }
    }

    public Map<Range<Token>, Integer> getRepairHistoryForRanges(Collection<Range<Token>> ranges)
    {
        Map<Range<Token>, Integer> history = new HashMap<>();

        for (Pair<Range<Token>, Integer> lastRepairSuccess : lastSuccessfulRepair.get().successfulRepairs)
        {
            if (lastRepairSuccess.left.intersects(ranges))
            {
                history.put(lastRepairSuccess.left, lastRepairSuccess.right);
            }
        }

        return history;
    }

    public int gcBefore(int nowInSec)
    {
        return nowInSec - metadata.params.gcGraceSeconds;
    }

    @SuppressWarnings("resource")
    public RefViewFragment selectAndReference(Function<View, Iterable<SSTableReader>> filter)
    {
        long failingSince = -1L;
        while (true)
        {
            ViewFragment view = select(filter);
            Refs<SSTableReader> refs = Refs.tryRef(view.sstables);
            if (refs != null)
                return new RefViewFragment(view.sstables, view.memtables, refs);
            if (failingSince <= 0)
            {
                failingSince = System.nanoTime();
            }
            else if (System.nanoTime() - failingSince > TimeUnit.MILLISECONDS.toNanos(100))
            {
                List<SSTableReader> released = new ArrayList<>();
                for (SSTableReader reader : view.sstables)
                    if (reader.selfRef().globalCount() == 0)
                        released.add(reader);
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                 "Spinning trying to capture readers {}, released: {}, ", view.sstables, released);
                failingSince = System.nanoTime();
            }
        }
    }

    public ViewFragment select(Function<View, Iterable<SSTableReader>> filter)
    {
        View view = data.getView();
        List<SSTableReader> sstables = Lists.newArrayList(filter.apply(view));
        return new ViewFragment(sstables, view.getAllMemtables());
    }

    // WARNING: this returns the set of LIVE sstables only, which may be only partially written
    public List<String> getSSTablesForKey(String key)
    {
        DecoratedKey dk = decorateKey(metadata.getKeyValidator().fromString(key));
        try (OpOrder.Group op = readOrdering.start())
        {
            List<String> files = new ArrayList<>();
            for (SSTableReader sstr : select(View.select(SSTableSet.LIVE, dk)).sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.getPosition(dk, SSTableReader.Operator.EQ, false) != null)
                    files.add(sstr.getFilename());
            }
            return files;
        }
    }


    public void beginLocalSampling(String sampler, int capacity)
    {
        metric.samplers.get(Sampler.valueOf(sampler)).beginSampling(capacity);
    }

    public CompositeData finishLocalSampling(String sampler, int count) throws OpenDataException
    {
        SamplerResult<ByteBuffer> samplerResults = metric.samplers.get(Sampler.valueOf(sampler))
                .finishSampling(count);
        TabularDataSupport result = new TabularDataSupport(COUNTER_TYPE);
        for (Counter<ByteBuffer> counter : samplerResults.topK)
        {
            byte[] key = counter.getItem().array();
            result.put(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES, new Object[] {
                    Hex.bytesToHex(key), // raw
                    counter.getCount(),  // count
                    counter.getError(),  // error
                    metadata.getKeyValidator().getString(ByteBuffer.wrap(key)) })); // string
        }
        return new CompositeDataSupport(SAMPLING_RESULT, SAMPLER_NAMES, new Object[]{
                samplerResults.cardinality, result});
    }

    public boolean isCompactionDiskSpaceCheckEnabled()
    {
        return compactionSpaceCheck;
    }

    public void compactionDiskSpaceCheck(boolean enable)
    {
        compactionSpaceCheck = enable;
    }

    public void cleanupCache()
    {
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());

        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.ksAndCFName.equals(metadata.ksAndCFName) && !Range.isInRanges(dk.getToken(), ranges))
                invalidateCachedPartition(dk);
        }

        if (metadata.isCounter())
        {
            for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
                 keyIter.hasNext(); )
            {
                CounterCacheKey key = keyIter.next();
                DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.partitionKey));
                if (key.ksAndCFName.equals(metadata.ksAndCFName) && !Range.isInRanges(dk.getToken(), ranges))
                    CacheService.instance.counterCache.remove(key);
            }
        }
    }

    public ClusteringComparator getComparator()
    {
        return metadata.comparator;
    }

    public void snapshotWithoutFlush(String snapshotName)
    {
        snapshotWithoutFlush(snapshotName, null, false);
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned during next startup
     */
    public Set<SSTableReader> snapshotWithoutFlush(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral)
    {
        Set<SSTableReader> snapshottedSSTables = new HashSet<>();
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            final JSONArray filesJSONArr = new JSONArray();
            try (RefViewFragment currentView = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.getPath()); // hard links
                    filesJSONArr.add(ssTable.descriptor.relativeFilenameFor(Component.DATA));

                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", keyspace, ssTable.getFilename(), snapshotDirectory);
                    snapshottedSSTables.add(ssTable);
                }

                writeSnapshotManifest(filesJSONArr, snapshotName);

                if (!Schema.isLocalSystemKeyspace(metadata.ksName) && !Schema.isReplicatedSystemKeyspace(metadata.ksName))
                    writeSnapshotSchema(snapshotName);
            }
        }
        if (ephemeral)
            createEphemeralSnapshotMarkerFile(snapshotName);
        return snapshottedSSTables;
    }

    private void writeSnapshotManifest(final JSONArray filesJSONArr, final String snapshotName)
    {
        final File manifestFile = getDirectories().getSnapshotManifestFile(snapshotName);

        try
        {
            if (!manifestFile.getParentFile().exists())
                manifestFile.getParentFile().mkdirs();

            try (PrintStream out = new PrintStream(manifestFile))
            {
                final JSONObject manifestJSON = new JSONObject();
                manifestJSON.put("files", filesJSONArr);
                out.println(manifestJSON.toJSONString());
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, manifestFile);
        }
    }

    private void writeSnapshotSchema(final String snapshotName)
    {
        final File schemaFile = getDirectories().getSnapshotSchemaFile(snapshotName);

        try
        {
            if (!schemaFile.getParentFile().exists())
                schemaFile.getParentFile().mkdirs();

            try (PrintStream out = new PrintStream(schemaFile))
            {
                for (String s: ColumnFamilyStoreCQLHelper.dumpReCreateStatements(metadata))
                    out.println(s);
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, schemaFile);
        }
    }

    private void createEphemeralSnapshotMarkerFile(final String snapshot)
    {
        final File ephemeralSnapshotMarker = getDirectories().getNewEphemeralSnapshotMarkerFile(snapshot);

        try
        {
            if (!ephemeralSnapshotMarker.getParentFile().exists())
                ephemeralSnapshotMarker.getParentFile().mkdirs();

            Files.createFile(ephemeralSnapshotMarker.toPath());
            logger.trace("Created ephemeral snapshot marker file on {}.", ephemeralSnapshotMarker.getAbsolutePath());
        }
        catch (IOException e)
        {
            logger.warn(String.format("Could not create marker file %s for ephemeral snapshot %s. " +
                                      "In case there is a failure in the operation that created " +
                                      "this snapshot, you may need to clean it manually afterwards.",
                                      ephemeralSnapshotMarker.getAbsolutePath(), snapshot), e);
        }
    }

    protected static void clearEphemeralSnapshots(Directories directories)
    {
        for (String ephemeralSnapshot : directories.listEphemeralSnapshots())
        {
            logger.trace("Clearing ephemeral snapshot {} leftover from previous session.", ephemeralSnapshot);
            Directories.clearSnapshot(ephemeralSnapshot, directories.getCFDirectories());
        }
    }

    public Refs<SSTableReader> getSnapshotSSTableReaders(String tag) throws IOException
    {
        Map<Integer, SSTableReader> active = new HashMap<>();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            active.put(sstable.descriptor.generation, sstable);
        Map<Descriptor, Set<Component>> snapshots = getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(tag).list();
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (Map.Entry<Descriptor, Set<Component>> entries : snapshots.entrySet())
            {
                // Try acquire reference to an active sstable instead of snapshot if it exists,
                // to avoid opening new sstables. If it fails, use the snapshot reference instead.
                SSTableReader sstable = active.get(entries.getKey().generation);
                if (sstable == null || !refs.tryRef(sstable))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("using snapshot sstable {}", entries.getKey());
                    // open offline so we don't modify components or track hotness.
                    sstable = SSTableReader.open(entries.getKey(), entries.getValue(), metadata, true, true);
                    refs.tryRef(sstable);
                    // release the self ref as we never add the snapshot sstable to DataTracker where it is otherwise released
                    sstable.selfRef().release();
                }
                else if (logger.isTraceEnabled())
                {
                    logger.trace("using active sstable {}", entries.getKey());
                }
            }
        }
        catch (IOException | RuntimeException e)
        {
            // In case one of the snapshot sstables fails to open,
            // we must release the references to the ones we opened so far
            refs.release();
            throw e;
        }
        return refs;
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     */
    public Set<SSTableReader> snapshot(String snapshotName)
    {
        return snapshot(snapshotName, null, false);
    }


    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     */
    public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral)
    {
        forceBlockingFlush();
        return snapshotWithoutFlush(snapshotName, predicate, ephemeral);
    }

    public boolean snapshotExists(String snapshotName)
    {
        return getDirectories().snapshotExists(snapshotName);
    }

    public long getSnapshotCreationTime(String snapshotName)
    {
        return getDirectories().snapshotCreationTime(snapshotName);
    }

    /**
     * Clear all the snapshots for a given column family.
     *
     * @param snapshotName the user supplied snapshot name. If left empty,
     *                     all the snapshots will be cleaned.
     */
    public void clearSnapshot(String snapshotName)
    {
        List<File> snapshotDirs = getDirectories().getCFDirectories();
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }
    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has true size and size on disk.
     */
    public Map<String, Pair<Long,Long>> getSnapshotDetails()
    {
        return getDirectories().getSnapshotDetails();
    }

    /**
     * @return the cached partition for @param key if it is already present in the cache.
     * Not that this will not readAndCache the parition if it is not present, nor
     * are these calls counted in cache statistics.
     *
     * Note that this WILL cause deserialization of a SerializingCache partition, so if all you
     * need to know is whether a partition is present or not, use containsCachedParition instead.
     */
    public CachedPartition getRawCachedPartition(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return null;
        IRowCacheEntry cached = CacheService.instance.rowCache.getInternal(new RowCacheKey(metadata.ksAndCFName, key));
        return cached == null || cached instanceof RowCacheSentinel ? null : (CachedPartition)cached;
    }

    private void invalidateCaches()
    {
        CacheService.instance.invalidateKeyCacheForCf(metadata.ksAndCFName);
        CacheService.instance.invalidateRowCacheForCf(metadata.ksAndCFName);
        if (metadata.isCounter())
            CacheService.instance.invalidateCounterCacheForCf(metadata.ksAndCFName);
    }

    public int invalidateRowCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.ksAndCFName.equals(metadata.ksAndCFName) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
            {
                invalidateCachedPartition(dk);
                invalidatedKeys++;
            }
        }
        return invalidatedKeys;
    }

    public int invalidateCounterCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
             keyIter.hasNext(); )
        {
            CounterCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.partitionKey));
            if (key.ksAndCFName.equals(metadata.ksAndCFName) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
            {
                CacheService.instance.counterCache.remove(key);
                invalidatedKeys++;
            }
        }
        return invalidatedKeys;
    }

    /**
     * @return true if @param key is contained in the row cache
     */
    public boolean containsCachedParition(DecoratedKey key)
    {
        return CacheService.instance.rowCache.getCapacity() != 0 && CacheService.instance.rowCache.containsKey(new RowCacheKey(metadata.ksAndCFName, key));
    }

    public void invalidateCachedPartition(RowCacheKey key)
    {
        CacheService.instance.rowCache.remove(key);
    }

    public void invalidateCachedPartition(DecoratedKey key)
    {
        if (!Schema.instance.hasCF(metadata.ksAndCFName))
            return; //2i don't cache rows

        invalidateCachedPartition(new RowCacheKey(metadata.ksAndCFName, key));
    }

    public ClockAndCount getCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnDefinition column, CellPath path)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return null;
        return CacheService.instance.counterCache.get(CounterCacheKey.create(metadata.ksAndCFName, partitionKey, clustering, column, path));
    }

    public void putCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnDefinition column, CellPath path, ClockAndCount clockAndCount)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return;
        CacheService.instance.counterCache.put(CounterCacheKey.create(metadata.ksAndCFName, partitionKey, clustering, column, path), clockAndCount);
    }

    public void forceMajorCompaction() throws InterruptedException, ExecutionException
    {
        forceMajorCompaction(false);
    }

    public void forceMajorCompaction(boolean splitOutput) throws InterruptedException, ExecutionException
    {
        CompactionManager.instance.performMaximal(this, splitOutput);
    }

    public void forceCompactionForTokenRange(Collection<Range<Token>> tokenRanges) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.forceCompactionForTokenRange(this, tokenRanges);
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<Iterable<ColumnFamilyStore>>(Schema.instance.getKeyspaces().size());
        for (Keyspace keyspace : Keyspace.all())
        {
            stores.add(keyspace.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public Iterable<DecoratedKey> keySamples(Range<Token> range)
    {
        try (RefViewFragment view = selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            Iterable<DecoratedKey>[] samples = new Iterable[view.sstables.size()];
            int i = 0;
            for (SSTableReader sstable: view.sstables)
            {
                samples[i++] = sstable.getKeySamples(range);
            }
            return Iterables.concat(samples);
        }
    }

    public long estimatedKeysForRange(Range<Token> range)
    {
        try (RefViewFragment view = selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            long count = 0;
            for (SSTableReader sstable : view.sstables)
                count += sstable.estimatedKeysForRanges(Collections.singleton(range));
            return count;
        }
    }

    /**
     * For testing.  No effort is made to clear historical or even the current memtables, nor for
     * thread safety.  All we do is wipe the sstable containers clean, while leaving the actual
     * data files present on disk.  (This allows tests to easily call loadNewSSTables on them.)
     */
    @VisibleForTesting
    public void clearUnsafe()
    {
        for (final ColumnFamilyStore cfs : concatWithIndexes())
        {
            cfs.runWithCompactionsDisabled(new Callable<Void>()
            {
                public Void call()
                {
                    cfs.data.reset(new Memtable(new AtomicReference<>(ReplayPosition.NONE), cfs));
                    return null;
                }
            }, true, false);
        }
    }

    /**
     * Truncate deletes the entire column family's data with no expensive tombstone creation
     */
    public void truncateBlocking()
    {
        // We have two goals here:
        // - truncate should delete everything written before truncate was invoked
        // - but not delete anything that isn't part of the snapshot we create.
        // We accomplish this by first flushing manually, then snapshotting, and
        // recording the timestamp IN BETWEEN those actions. Any sstables created
        // with this timestamp or greater time, will not be marked for delete.
        //
        // Bonus complication: since we store replay position in sstable metadata,
        // truncating those sstables means we will replay any CL segments from the
        // beginning if we restart before they [the CL segments] are discarded for
        // normal reasons post-truncate.  To prevent this, we store truncation
        // position in the System keyspace.
        logger.trace("truncating {}", name);

        final long truncatedAt;
        final ReplayPosition replayAfter;

        if (keyspace.getMetadata().params.durableWrites || DatabaseDescriptor.isAutoSnapshot())
        {
            replayAfter = forceBlockingFlush();
            viewManager.forceBlockingFlush();
        }
        else
        {
            // just nuke the memtable data w/o writing to disk first
            viewManager.dumpMemtables();
            try
            {
                replayAfter = dumpMemtable().get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        long now = System.currentTimeMillis();
        // make sure none of our sstables are somehow in the future (clock drift, perhaps)
        for (ColumnFamilyStore cfs : concatWithIndexes())
            for (SSTableReader sstable : cfs.getLiveSSTables())
                now = Math.max(now, sstable.maxDataAge);
        truncatedAt = now;

        Runnable truncateRunnable = new Runnable()
        {
            public void run()
            {
                logger.debug("Discarding sstable data for truncated CF + indexes");
                data.notifyTruncated(truncatedAt);

                if (DatabaseDescriptor.isAutoSnapshot())
                    snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(name, SNAPSHOT_TRUNCATE_PREFIX));

                discardSSTables(truncatedAt);

                indexManager.truncateAllIndexesBlocking(truncatedAt);
                viewManager.truncateBlocking(replayAfter, truncatedAt);

                SystemKeyspace.saveTruncationRecord(ColumnFamilyStore.this, truncatedAt, replayAfter);
                logger.trace("cleaning out row cache");
                invalidateCaches();
            }
        };

        runWithCompactionsDisabled(Executors.callable(truncateRunnable), true, true);
        logger.trace("truncate complete");
    }

    /**
     * Drops current memtable without flushing to disk. This should only be called when truncating a column family which is not durable.
     */
    public Future<ReplayPosition> dumpMemtable()
    {
        synchronized (data)
        {
            final Flush flush = new Flush(true);
            flushExecutor.execute(flush);
            return postFlushExecutor.submit(flush.postFlush);
        }
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews)
    {
        return runWithCompactionsDisabled(callable, (sstable) -> true, interruptValidation, interruptViews);
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, Predicate<SSTableReader> sstablesPredicate, boolean interruptValidation, boolean interruptViews)
    {
        // synchronize so that concurrent invocations don't re-enable compactions partway through unexpectedly,
        // and so we only run one major compaction at a time
        synchronized (this)
        {
            logger.trace("Cancelling in-progress compactions for {}", metadata.cfName);

            Iterable<ColumnFamilyStore> selfWithAuxiliaryCfs = interruptViews
                                                               ? Iterables.concat(concatWithIndexes(), viewManager.allViewsCfs())
                                                               : concatWithIndexes();

            for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                cfs.getCompactionStrategyManager().pause();
            try
            {
                // interrupt in-progress compactions
                CompactionManager.instance.interruptCompactionForCFs(selfWithAuxiliaryCfs, sstablesPredicate, interruptValidation);
                CompactionManager.instance.waitForCessation(selfWithAuxiliaryCfs, sstablesPredicate);

                // doublecheck that we finished, instead of timing out
                for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                {
                    if (Iterables.any(cfs.getTracker().getCompacting(), sstablesPredicate))
                    {
                        logger.warn("Unable to cancel in-progress compactions for {}.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.", metadata.cfName);
                        return null;
                    }
                }
                logger.trace("Compactions successfully cancelled");

                // run our task
                try
                {
                    return callable.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
            finally
            {
                for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                    cfs.getCompactionStrategyManager().resume();
            }
        }
    }

    public LifecycleTransaction markAllCompacting(final OperationType operationType)
    {
        Callable<LifecycleTransaction> callable = new Callable<LifecycleTransaction>()
        {
            public LifecycleTransaction call() throws Exception
            {
                assert data.getCompacting().isEmpty() : data.getCompacting();
                Iterable<SSTableReader> sstables = getLiveSSTables();
                sstables = AbstractCompactionStrategy.filterSuspectSSTables(sstables);
                LifecycleTransaction modifier = data.tryModify(sstables, operationType);
                assert modifier != null: "something marked things compacting while compactions are disabled";
                return modifier;
            }
        };

        return runWithCompactionsDisabled(callable, false, false);
    }


    @Override
    public String toString()
    {
        return "CFS(" +
               "Keyspace='" + keyspace.getName() + '\'' +
               ", ColumnFamily='" + name + '\'' +
               ')';
    }

    public void disableAutoCompaction()
    {
        // we don't use CompactionStrategy.pause since we don't want users flipping that on and off
        // during runWithCompactionsDisabled
        compactionStrategyManager.disable();
    }

    public void enableAutoCompaction()
    {
        enableAutoCompaction(false);
    }

    /**
     * used for tests - to be able to check things after a minor compaction
     * @param waitForFutures if we should block until autocompaction is done
     */
    @VisibleForTesting
    public void enableAutoCompaction(boolean waitForFutures)
    {
        compactionStrategyManager.enable();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(this);
        if (waitForFutures)
            FBUtilities.waitOnFutures(futures);
    }

    public boolean isAutoCompactionDisabled()
    {
        return !this.compactionStrategyManager.isEnabled();
    }

    /*
     JMX getters and setters for the Default<T>s.
       - get/set minCompactionThreshold
       - get/set maxCompactionThreshold
       - get     memsize
       - get     memops
       - get/set memtime
     */

    public CompactionStrategyManager getCompactionStrategyManager()
    {
        return compactionStrategyManager;
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        try
        {
            TableParams.builder().crcCheckChance(crcCheckChance).build().validate();
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                cfs.crcCheckChance.set(crcCheckChance);
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    sstable.setCrcCheckChance(crcCheckChance);
            }
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }


    public Double getCrcCheckChance()
    {
        return crcCheckChance.value();
    }

    public void setCompactionThresholds(int minThreshold, int maxThreshold)
    {
        validateCompactionThresholds(minThreshold, maxThreshold);

        minCompactionThreshold.set(minThreshold);
        maxCompactionThreshold.set(maxThreshold);
        CompactionManager.instance.submitBackground(this);
    }

    public int getMinimumCompactionThreshold()
    {
        return minCompactionThreshold.value();
    }

    public void setMinimumCompactionThreshold(int minCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold, maxCompactionThreshold.value());
        this.minCompactionThreshold.set(minCompactionThreshold);
    }

    public int getMaximumCompactionThreshold()
    {
        return maxCompactionThreshold.value();
    }

    public void setMaximumCompactionThreshold(int maxCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold.value(), maxCompactionThreshold);
        this.maxCompactionThreshold.set(maxCompactionThreshold);
    }

    private void validateCompactionThresholds(int minThreshold, int maxThreshold)
    {
        if (minThreshold > maxThreshold)
            throw new RuntimeException(String.format("The min_compaction_threshold cannot be larger than the max_compaction_threshold. " +
                                                     "Min is '%d', Max is '%d'.", minThreshold, maxThreshold));

        if (maxThreshold == 0 || minThreshold == 0)
            throw new RuntimeException("Disabling compaction by setting min_compaction_threshold or max_compaction_threshold to 0 " +
                    "is deprecated, set the compaction strategy option 'enabled' to 'false' instead or use the nodetool command 'disableautocompaction'.");
    }

    // End JMX get/set.

    public int getMeanColumns()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedColumnCount().count();
            sum += sstable.getEstimatedColumnCount().mean() * n;
            count += n;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public double getMeanPartitionSize()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedPartitionSize().count();
            sum += sstable.getEstimatedPartitionSize().mean() * n;
            count += n;
        }
        return count > 0 ? sum * 1.0 / count : 0;
    }

    public long estimateKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            n += sstable.estimatedKeys();
        return n;
    }

    public IPartitioner getPartitioner()
    {
        return metadata.partitioner;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return metadata.decorateKey(key);
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return metadata.isIndex();
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        // we return the main CFS first, which we rely on for simplicity in switchMemtable(), for getting the
        // latest replay position
        return Iterables.concat(Collections.singleton(this), indexManager.getAllIndexColumnFamilyStores());
    }

    public List<String> getBuiltIndexes()
    {
       return indexManager.getBuiltIndexNames();
    }

    public int getUnleveledSSTables()
    {
        return this.compactionStrategyManager.getUnleveledSSTables();
    }

    public int[] getSSTableCountPerLevel()
    {
        return compactionStrategyManager.getSSTableCountPerLevel();
    }

    private volatile long gcBeforeSeconds;
    private volatile long lastCalculatedTime = 0;
    private static long REFRESH_TIME_MILLIS = 300L * 1000L;

    /*
        This method returns Long.MAX_VALUE if repaired ranges are not there.
     */
    private long getLastMinRepairSuccessTime()
    {
        final Map<Range<Token>, Integer> repairMap = SystemKeyspace.getLastSuccessfulRepair(keyspace.getName(), name);
        long lowestRepairTime = Long.MAX_VALUE;
        for (final int repairTime : repairMap.values())
        {
            if (repairTime < lowestRepairTime)
                lowestRepairTime = repairTime;
        }
            return lowestRepairTime;
    }

    // this is only used for metrics, basically how old the oldest repair is, not merging in invalidations since we
    // only want to know how old the oldest repair is
    public long getGCBeforeSeconds()
    {
        if ((System.currentTimeMillis() - lastCalculatedTime) > REFRESH_TIME_MILLIS)
        {
            gcBeforeSeconds = ((System.currentTimeMillis() / 1000) - getLastMinRepairSuccessTime());
            lastCalculatedTime = System.currentTimeMillis();
            if(gcBeforeSeconds < 0)
                gcBeforeSeconds = 0;
        }

            return gcBeforeSeconds;
    }

    public static class ViewFragment
    {
        public final List<SSTableReader> sstables;
        public final Iterable<Memtable> memtables;

        public ViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables)
        {
            this.sstables = sstables;
            this.memtables = memtables;
        }
    }

    public static class RefViewFragment extends ViewFragment implements AutoCloseable
    {
        public final Refs<SSTableReader> refs;

        public RefViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables, Refs<SSTableReader> refs)
        {
            super(sstables, memtables);
            this.refs = refs;
        }

        public void release()
        {
            refs.release();
        }

        public void close()
        {
            refs.release();
        }
    }

    public boolean isEmpty()
    {
        return data.getView().isEmpty();
    }

    public boolean isRowCacheEnabled()
    {

        boolean retval = metadata.params.caching.cacheRows() && CacheService.instance.rowCache.getCapacity() > 0;
        assert(!retval || !isIndex());
        return retval;
    }

    public boolean isCounterCacheEnabled()
    {
        return metadata.isCounter() && CacheService.instance.counterCache.getCapacity() > 0;
    }

    public boolean isKeyCacheEnabled()
    {
        return metadata.params.caching.cacheKeys() && CacheService.instance.keyCache.getCapacity() > 0;
    }

    /**
     * Discard all SSTables that were created before given timestamp.
     *
     * Caller should first ensure that comapctions have quiesced.
     *
     * @param truncatedAt The timestamp of the truncation
     *                    (all SSTables before that timestamp are going be marked as compacted)
     */
    public void discardSSTables(long truncatedAt)
    {
        assert data.getCompacting().isEmpty() : data.getCompacting();

        List<SSTableReader> truncatedSSTables = new ArrayList<>();

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            if (!sstable.newSince(truncatedAt))
                truncatedSSTables.add(sstable);
        }

        if (!truncatedSSTables.isEmpty())
            markObsolete(truncatedSSTables, OperationType.UNKNOWN);
    }

    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(System.currentTimeMillis()/1000);

        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - sstable.metadata.params.gcGraceSeconds);
            allColumns += sstable.getEstimatedColumnCount().mean() * sstable.getEstimatedColumnCount().count();
        }
        return allColumns > 0 ? allDroppable / allColumns : 0;
    }

    public long trueSnapshotsSize()
    {
        return getDirectories().trueSnapshotsSize();
    }

    @VisibleForTesting
    void resetFileIndexGenerator()
    {
        fileIndexGenerator.set(0);
    }

    /**
     * Returns a ColumnFamilyStore by cfId if it exists, null otherwise
     * Differently from others, this method does not throw exception if the table does not exist.
     */
    public static ColumnFamilyStore getIfExists(UUID cfId)
    {
        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        if (kscf == null)
            return null;

        Keyspace keyspace = Keyspace.open(kscf.left);
        if (keyspace == null)
            return null;

        return keyspace.getColumnFamilyStore(cfId);
    }

    /**
     * Returns a ColumnFamilyStore by ksname and cfname if it exists, null otherwise
     * Differently from others, this method does not throw exception if the keyspace or table does not exist.
     */
    public static ColumnFamilyStore getIfExists(String ksName, String cfName)
    {
        if (ksName == null || cfName == null)
            return null;

        Keyspace keyspace = Keyspace.open(ksName);
        if (keyspace == null)
            return null;

        UUID id = Schema.instance.getId(ksName, cfName);
        if (id == null)
            return null;

        return keyspace.getColumnFamilyStore(id);
    }

    @Override
    public long[] getRecentSSTablesPerReadHistogramV3()
    {
        return metric.recentSSTablesPerRead.getBuckets(true);
    }

    @Override
    public long[] getSSTablesPerReadHistogramV3()
    {
        return metric.sstablesPerRead.getBuckets(false);
    }

    @Override
    public long[] getRecentReadLatencyHistogramMicrosV3()
    {
        return metric.readLatency.recentLatencyHistogram.getBuckets(true);
    }

    @Override
    public long[] getRecentWriteLatencyHistogramMicrosV3()
    {
        return metric.writeLatency.recentLatencyHistogram.getBuckets(true);
    }

    @Override
    public long[] getRecentRangeLatencyHistogramMicrosV3()
    {
        return metric.rangeLatency.recentLatencyHistogram.getBuckets(true);
    }

    @Override
    public long[] getEstimatedRowSizeHistogram()
    {
        return metric.estimatedPartitionSizeHistogram.getValue();
    }

    @Override
    public long[] getEstimatedColumnCountHistogram()
    {
        return metric.estimatedColumnCountHistogram.getValue();
    }

    @Override
    public long[] getCasPrepareLatencyHistogram() {
        return metric.casPrepare.recentLatencyHistogram.getBuckets(true);
    }

    @Override
    public long[] getCasProposeLatencyHistogram() {
        return metric.casPropose.recentLatencyHistogram.getBuckets(true);
    }

    @Override
    public long[] getCasCommitLatencyHistogram() {
        return metric.casCommit.recentLatencyHistogram.getBuckets(true);
    }

    @Override
    public long[] getCoordinatorScanLatencyHistogram()
    {
        return metric.coordinatorScanLatencyNanos.recentLatencyHistogram.getBuckets(true);
    }

    public static TableMetrics metricsFor(UUID tableId)
    {
        return getIfExists(tableId).metric;
    }

    @Override
    public void setNeverPurgeTombstones(boolean value)
    {
        if (neverPurgeTombstones != value)
            logger.info("Changing neverPurgeTombstones for {}.{} from {} to {}", keyspace.getName(), getTableName(), neverPurgeTombstones, value);
        else
            logger.info("Not changing neverPurgeTombstones for {}.{}, it is {}", keyspace.getName(), getTableName(), neverPurgeTombstones);

        neverPurgeTombstones = value;
    }

    @Override
    public boolean getNeverPurgeTombstones()
    {
        return neverPurgeTombstones;
    }
}