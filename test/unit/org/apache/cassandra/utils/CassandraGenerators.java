package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.SuperColumnCompatibility;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.net.PingMessage;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.AbstractTypeGenerators.allowReversed;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.typeGen;
import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;

public final class CassandraGenerators
{
    // utility generators for creating more complex types
    // cie - use even smaller values!  4.0 allows larger types in serialization than 3.0 so need this to be smaller
    private static final Gen<Integer> REALLY_SMALL_POSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 3);
    private static final Gen<Integer> SMALL_POSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 30);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();

    private static final Gen<IPartitioner> PARTITIONER_GEN = SourceDSL.arbitrary().pick(Murmur3Partitioner.instance,
                                                                                        ByteOrderedPartitioner.instance,
                                                                                        new LocalPartitioner(TimeUUIDType.instance),
                                                                                        OrderPreservingPartitioner.instance,
                                                                                        RandomPartitioner.instance);

    public static final Gen<CFMetaData> TABLE_METADATA_GEN = rnd -> createTableMetadata(IDENTIFIER_GEN.generate(rnd), rnd);

    private static final Gen<SinglePartitionReadCommand> SINGLE_PARTITION_READ_COMMAND_GEN = rnd -> {
        CFMetaData metadata = TABLE_METADATA_GEN.generate(rnd);
        int nowInSec = (int) rnd.next(Constraint.between(1, Integer.MAX_VALUE));
        Gen<ByteBuffer> pkGen = partitionKeyDataGen(metadata);
        // in 4.0 we moved to vint when encoding bytes, so 4.0 allows 2gb partition key
        // in 3.0 we use short when encoding bytes, so max of 65,535 bytes
        pkGen = Generators.filter(pkGen, 20, b -> b.remaining() <= FBUtilities.MAX_UNSIGNED_SHORT);
        ByteBuffer key = pkGen.generate(rnd);
        //TODO support all fields of SinglePartitionReadCommand
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, Slices.ALL);
    };
    private static final Gen<? extends ReadCommand> READ_COMMAND_GEN = Generate.oneOf(SINGLE_PARTITION_READ_COMMAND_GEN);

    // Outbound messages
    private static final Gen<OutboundTcpConnectionPool.ConnectionType> CONNECTION_TYPE_GEN = SourceDSL.arbitrary().enumValues(OutboundTcpConnectionPool.ConnectionType.class);
    public static final Gen<MessageOut<PingMessage>> MESSAGE_PING_GEN = rnd -> new MessageOut<>(MessagingService.Verb.PING, new PingMessage(CONNECTION_TYPE_GEN.generate(rnd)), PingMessage.serializer);
    public static final Gen<MessageOut<ReadCommand>> MESSAGE_READ_COMMAND_GEN = rnd -> new MessageOut<>(MessagingService.Verb.READ, READ_COMMAND_GEN.generate(rnd), ReadCommand.readSerializer);

    /**
     * Hacky workaround to make sure different generic MessageOut types can be used for {@link #MESSAGE_GEN}.
     */
    private static final Gen<MessageOut<?>> cast(Gen<? extends MessageOut<?>> gen)
    {
        return (Gen<MessageOut<?>>) gen;
    }

    public static final Gen<MessageOut<?>> MESSAGE_GEN = Generate.oneOf(cast(MESSAGE_PING_GEN),
                                                                     cast(MESSAGE_READ_COMMAND_GEN));

    private CassandraGenerators()
    {

    }

    private static CFMetaData createTableMetadata(String ks, RandomnessSource rnd)
    {
        String tableName = IDENTIFIER_GEN.generate(rnd);
        UUID id = Generators.UUID_RANDOM_GEN.generate(rnd);
        boolean isDense = BOOLEAN_GEN.generate(rnd);
        boolean isCompound = BOOLEAN_GEN.generate(rnd);
        boolean isSuper = BOOLEAN_GEN.generate(rnd);
        boolean isCounter = BOOLEAN_GEN.generate(rnd);
        boolean isView = BOOLEAN_GEN.generate(rnd);
        IPartitioner partitioner = PARTITIONER_GEN.generate(rnd);

        // generate columns
        // must have a non-zero amount of partition columns, but may have 0 for the rest; SMALL_POSSITIVE_SIZE_GEN won't return 0
        int numPartitionColumns = REALLY_SMALL_POSITIVE_SIZE_GEN.generate(rnd); // cie - partition must be less than max unsigned int, which gets really hard with wide schemas
        int numClusteringColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd) - 1;
        int numRegularColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd) - 1;
        int numStaticColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd) - 1;

        boolean isCQLTable = !isSuper && !isDense && isCompound;
        boolean isCompactTable = !isCQLTable;

        List<ColumnDefinition> columns = new ArrayList<>(numPartitionColumns + numClusteringColumns + numRegularColumns);
        AbstractType<?> superColumnKeyType = null;
        if (isCompactTable)
        {
            if (isSuper)
            {
                // need a compact value column
                superColumnKeyType = typeGen().generate(rnd);
                columns.add(ColumnDefinition.regularDef(ks, tableName, SuperColumnCompatibility.SUPER_COLUMN_MAP_COLUMN_STR, MapType.getInstance(superColumnKeyType, typeGen().generate(rnd), true)));
                if (numRegularColumns == 0)
                {
                    // only the column added is present, so this is dense now...
                    isDense = true;
                }
            }
            else
            {
                // compact tables have a compact value, so generate one
                columns.add(new ColumnDefinition(ks, tableName, new ColumnIdentifier("value", true), EmptyType.instance, -1, ColumnDefinition.Kind.REGULAR));
                // compact tables have a compact value, so need to follow the expected rules
                // the rules are handled after creating the columns, for now make regular == 0
                numRegularColumns = 0;
                numStaticColumns = 0;
            }

            if (isSuper && isDense)
            {
                // super column requires 1-2 clustering columns (see org.apache.cassandra.cql3.SuperColumnCompatibility.getSuperCfKeyColumn)
                numClusteringColumns = (numClusteringColumns % 2) + 1; // generates 1 or 2
            }
            if (numClusteringColumns == 0)
            {
                // compact tables require a clustering key
                numClusteringColumns = 1;
            }
        }

        Set<String> createdColumnNames = new HashSet<>();
        for (int i = 0; i < numPartitionColumns; i++)
            columns.add(createColumnDefinition(ks, tableName, ColumnDefinition.Kind.PARTITION_KEY, createdColumnNames, rnd));
        if (superColumnKeyType != null)
        {
            // super column map's key type is what the clustering columns must be of
            for (int i = 0; i < numClusteringColumns; i++)
                columns.add(createColumnDefinition(ks, tableName, ColumnDefinition.Kind.CLUSTERING, createdColumnNames, superColumnKeyType, rnd));
        }
        else
        {
            for (int i = 0; i < numClusteringColumns; i++)
                columns.add(createColumnDefinition(ks, tableName, ColumnDefinition.Kind.CLUSTERING, createdColumnNames, rnd));
        }
        for (int i = 0; i < numStaticColumns; i++)
            columns.add(createColumnDefinition(ks, tableName, ColumnDefinition.Kind.STATIC, createdColumnNames, rnd));
        for (int i = 0; i < numRegularColumns; i++)
            columns.add(createColumnDefinition(ks, tableName, ColumnDefinition.Kind.REGULAR, createdColumnNames, rnd));

        return CFMetaData.create(ks, tableName, id, isDense, isCompound, isSuper, isCounter, isView, columns, partitioner);
    }

    private static ColumnDefinition createColumnDefinition(String ks, String table,
                                                           ColumnDefinition.Kind kind,
                                                           Set<String> createdColumnNames, /* This is mutated to check for collisions, so has a side effect outside of normal random generation */
                                                           RandomnessSource rnd)
    {
        Gen<AbstractType<?>> typeGen = typeGen();
        switch (kind)
        {
            // partition and clustering keys require frozen types, so make sure all types generated will be frozen
            // empty type is also not supported, so filter out
            case PARTITION_KEY:
            case CLUSTERING:
                typeGen = Generators.filter(typeGen, t -> t != EmptyType.instance).map(AbstractType::freeze);
                break;
        }
        if (kind == ColumnDefinition.Kind.CLUSTERING)
        {
            // when working on a clustering column, add in reversed types periodically
            typeGen = allowReversed(typeGen);
        }
        return createColumnDefinition(ks, table, kind, createdColumnNames, typeGen.generate(rnd), rnd);
    }

    private static ColumnDefinition createColumnDefinition(String ks, String table,
                                                           ColumnDefinition.Kind kind,
                                                           Set<String> createdColumnNames, AbstractType<?> type,
                                                           RandomnessSource rnd)
    {
        // filter for unique names
        String str;
        while (!createdColumnNames.add(str = IDENTIFIER_GEN.generate(rnd)))
        {
        }
        ColumnIdentifier name = new ColumnIdentifier(str, true);
        int position = !kind.isPrimaryKeyKind() ? -1 : (int) rnd.next(Constraint.between(0, 30));
        return new ColumnDefinition(ks, table, name, type, position, kind);
    }

    public static Gen<ByteBuffer> partitionKeyDataGen(CFMetaData metadata)
    {
        List<ColumnDefinition> columns = metadata.partitionKeyColumns();
        assert !columns.isEmpty() : "Unable to find partition key columns";
        if (columns.size() == 1)
            return getTypeSupport(columns.get(0).type).bytesGen();
        List<Gen<ByteBuffer>> columnGens = new ArrayList<>(columns.size());
        for (ColumnDefinition cm : columns)
            columnGens.add(getTypeSupport(cm.type).bytesGen());
        return rnd -> {
            ByteBuffer[] buffers = new ByteBuffer[columnGens.size()];
            for (int i = 0; i < columnGens.size(); i++)
                buffers[i] = columnGens.get(i).generate(rnd);
            return CompositeType.build(buffers);
        };
    }
}
