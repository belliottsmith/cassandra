/**
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
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneVerifier
{
    private static final String TOOL_NAME = "sstableverify";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String EXTENDED_OPTION = "extended";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String CHECK_VERSION = "check_version";
    private static final String MUTATE_REPAIR_STATUS = "mutate_repair_status";
    private static final String QUICK = "quick";
    private static final String SSTABLES = "sstables";
    private static final String CREATE_STATEMENT = "create_statement";
    private static final String LOAD_SCHEMA_FROM_SSTABLE = "load_schema_from_sstable";
    private static final String VALIDATE_DATA = "validate_data";
    private static final String TOKEN_RANGE = "token_range";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        Util.initDatabaseDescriptor();
        System.out.println("sstableverify using the following options: " + options);
        try
        {
            Pair<ColumnFamilyStore, List<SSTableReader>> pair = getColumnFamilyStore(options);
            ColumnFamilyStore cfs = pair.left;
            List<SSTableReader> sstables = pair.right;

            boolean hasFailed = false;
            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);

            Verifier.Options verifyOptions = Verifier.options().invokeDiskFailurePolicy(false)
                                                     .extendedVerification(options.extended)
                                                     .checkVersion(options.checkVersion)
                                                     .mutateRepairStatus(options.mutateRepairStatus)
                                                     .checkOwnsTokens(false) // don't know the ranges when running offline
                                                     .checkOwnsTokens(!options.tokens.isEmpty())
                                                     .tokenLookup(ignore -> options.tokens)
                                                     .validateData(options.validateData)
                                                     .build();
            handler.output("Running verifier with the following options: " + verifyOptions);
            for (SSTableReader sstable : sstables)
            {
                try
                {

                    try (Verifier verifier = new Verifier(cfs, sstable, handler, true, verifyOptions))
                    {
                        verifier.verify();
                    }
                    catch (Exception cs)
                    {
                        System.err.println(String.format("Error verifying %s: %s", sstable, cs.getMessage()));
                        hasFailed = true;
                    }
                }
                catch (Exception e)
                {
                    System.err.println(String.format("Error verifying %s: %s", sstable, e.getMessage()));
                    e.printStackTrace(System.err);
                    hasFailed = true;
                }
            }

            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);

            System.exit(hasFailed ? 1 : 0); // We need that to stop non daemonized threads
        }
        catch (Exception e)
        {
            System.err.println("Unexpected error: " + e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static Pair<ColumnFamilyStore, List<SSTableReader>> getColumnFamilyStore(Options options)
    {
        if (options.sstables != null)
        {
            Config.setClientMode(true);
            Util.initDatabaseDescriptor();
            DatabaseDescriptor.setFlushWriters(1);
            DatabaseDescriptor.setConcurrentCompactors(1);
            DatabaseDescriptor.setConcurrentValidations(1);
            DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());

            List<Descriptor> descriptors = Stream.of(options.sstables)
                                                 .map(File::new)
                                                 .map(f -> {
                                                     if (!f.exists())
                                                         error("File " + f.getAbsolutePath() + " does not exist");

                                                     return Descriptor.fromFilename(f.getAbsolutePath());
                                                 })
                                                 .collect(Collectors.toList());

            Set<String> distinctTableNames = descriptors.stream().map(d -> d.ksname + "." + d.cfname).collect(Collectors.toSet());
            if (distinctTableNames.size() != 1)
                error("Only a single table is allowed, but found " + distinctTableNames);

            CFMetaData schema = loadSchema(options, descriptors);
            List<SSTableReader> sstables = descriptors.stream()
                                                      .map(d -> {
                                                          try
                                                          {
                                                              return SSTableReader.open(d, schema, true, true);
                                                          }
                                                          catch (IOException e)
                                                          {
                                                              JVMStabilityInspector.inspectThrowable(e);
                                                              System.err.println(String.format("Error Loading %s: %s", d, e.getMessage()));
                                                              if (options.debug)
                                                                  e.printStackTrace(System.err);
                                                              return null;
                                                          }
                                                      })
                                                      .filter(Objects::nonNull)
                                                      .collect(Collectors.toList());
            return Pair.create(mock(schema), sstables);
        }
        else
        {
            Util.initDatabaseDescriptor();

            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            if (Schema.instance.getCFMetaData(options.keyspaceName, options.cfName) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspaceName,
                                                                 options.cfName));

            // Do not load sstables since they might be broken
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cfName);

            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);

            List<SSTableReader> sstables = new ArrayList<>();

            // Verify sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA) || !components.contains(Component.PRIMARY_INDEX))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
                    sstables.add(sstable);
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }

            return Pair.create(cfs, sstables);
        }
    }

    private static ColumnFamilyStore mock(CFMetaData schema)
    {
        KeyspaceMetadata ksMeta = KeyspaceMetadata.create(schema.ksName, KeyspaceParams.local());
        Keyspace keyspace = Keyspace.mockKS(ksMeta);
        ColumnFamilyStore cfs = new ColumnFamilyStore(keyspace,
                                                      schema.cfName,
                                                      0,
                                                      schema,
                                                      new Directories(schema, new Directories.DataDirectory[0]),
                                                      false,
                                                      false);
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static RuntimeException error(String msg)
    {
        return error(msg, false);
    }

    private static RuntimeException error(String msg, boolean withUsage)
    {
        if (msg != null)
            System.err.println(msg);
        if (withUsage)
            Options.printUsage(Options.getCmdLineOptions());
        System.exit(1);
        throw new AssertionError("System.exit did not exit");
    }

    private static RuntimeException errorWithUsage(String msg)
    {
        return error(msg, true);
    }

    private static CFMetaData loadSchema(Options options, List<Descriptor> descriptors)
    {
        if (options.createStatement != null)
        {
            IPartitioner partitioner = loadPartitioner(descriptors.get(0));
            // partitioner is needed but not defined in client mode, so fake it
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            return CFMetaData.compile(options.createStatement, descriptors.get(0).ksname);
        }
        else if (options.loadSchemaFromSstable)
        {
            try
            {
                return SSTableExport.metadataFromSSTable(descriptors.get(0));
            }
            catch (IOException e)
            {
                throw error(e.getMessage());
            }
        }
        else
        {
            throw errorWithUsage("Unable to infer schema");
        }
    }

    private static IPartitioner loadPartitioner(Descriptor descriptor)
    {
        try
        {
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer()
                                                                      .deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
            return FBUtilities.construct(validation.partitioner, "partitioner");
        }
        catch (IOException e)
        {
            throw error(e.getMessage());
        }
    }

    private static class Options
    {
        public final String keyspaceName;
        public final String cfName;

        public boolean debug;
        public boolean verbose;
        public boolean extended;
        public boolean checkVersion;
        public boolean mutateRepairStatus;
        public boolean quick;
        public Collection<Range<Token>> tokens;
        public String[] sstables;
        public String createStatement;
        public boolean loadSchemaFromSstable;
        public boolean validateData;

        private Options(String keyspaceName, String cfName)
        {
            this.keyspaceName = keyspaceName;
            this.cfName = cfName;
        }

        public static Options parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length != 2)
                {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    System.err.println(msg);
                    printUsage(options);
                    System.exit(1);
                }

                String keyspaceName = args[0];
                String cfName = args[1];

                Options opts = new Options(keyspaceName, cfName);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.extended = cmd.hasOption(EXTENDED_OPTION);
                opts.checkVersion = cmd.hasOption(CHECK_VERSION);
                opts.mutateRepairStatus = cmd.hasOption(MUTATE_REPAIR_STATUS);
                opts.quick = cmd.hasOption(QUICK);
                if (cmd.hasOption(SSTABLES))
                    opts.sstables = Iterables.toArray(Splitter.on(",").trimResults().split(cmd.getOptionValue(SSTABLES)), String.class);
                if (cmd.hasOption(CREATE_STATEMENT))
                    opts.createStatement = cmd.getOptionValue(CREATE_STATEMENT);
                opts.loadSchemaFromSstable = cmd.hasOption(LOAD_SCHEMA_FROM_SSTABLE);
                opts.validateData = cmd.hasOption(VALIDATE_DATA);

                if (cmd.hasOption(TOKEN_RANGE))
                {
                    opts.tokens = Stream.of(cmd.getOptionValues(TOKEN_RANGE))
                                        .map(StandaloneVerifier::parseTokenRange)
                                        .collect(Collectors.toSet());
                }
                else
                {
                    opts.tokens = Collections.emptyList();
                }

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        public String toString()
        {
            return "Options{" +
                   "keyspaceName='" + keyspaceName + '\'' +
                   ", cfName='" + cfName + '\'' +
                   ", debug=" + debug +
                   ", verbose=" + verbose +
                   ", extended=" + extended +
                   ", checkVersion=" + checkVersion +
                   ", mutateRepairStatus=" + mutateRepairStatus +
                   ", quick=" + quick +
                   ", tokens=" + tokens +
                   '}';
        }

        private static void errorMsg(String msg, CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static CmdLineOptions getCmdLineOptions()
        {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption(null, DEBUG_OPTION,          "display stack traces");
            options.addOption("e",  EXTENDED_OPTION,       "extended verification");
            options.addOption("v",  VERBOSE_OPTION,        "verbose output");
            options.addOption("h",  HELP_OPTION,           "display this help message");
            options.addOption("c",  CHECK_VERSION,         "make sure sstables are the latest version");
            options.addOption("r",  MUTATE_REPAIR_STATUS,  "don't mutate repair status");
            options.addOption("q",  QUICK,                 "do a quick check, don't read all data");
            options.addOptionList("t", TOKEN_RANGE, "range", "long token range of the format left,right. This may be provided multiple times to define multiple different ranges");
            options.addOption("s", SSTABLES, true, "rather than loading SSTables from config, load from CLI");
            options.addOption("cs", CREATE_STATEMENT, true, "create statement defining the table schema");
            options.addOption("vd", VALIDATE_DATA, "validate the contents of the data");
            options.addOption(null, LOAD_SCHEMA_FROM_SSTABLE, "load schema from SSTables");
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <column_family>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Verify the sstable for the provided table.");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }

    private static Range<Token> parseTokenRange(String line)
    {
        String[] split = line.split(",");
        if (split.length != 2)
            throw new IllegalArgumentException("Unable to parse token range from " + line + "; format is left,right but saw " + split.length + " parts");
        long left = Long.parseLong(split[0]);
        long right = Long.parseLong(split[1]);
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }
}
