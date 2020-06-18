package org.apache.cassandra.transport.frame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.Frame.Header.Flag;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.transport.Frame.Header.Flag.CHECKSUMMED;
import static org.apache.cassandra.transport.messages.StartupMessage.CHECKSUM;
import static org.apache.cassandra.transport.messages.StartupMessage.COMPRESSION;
import static org.apache.cassandra.transport.messages.StartupMessage.USE_LEGACY_CHECKSUMS;
import static org.apache.cassandra.transport.messages.StartupMessage.getTransformer;
import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

@RunWith(Parameterized.class)
public class FrameBodyTransformerTest
{
    private static final EnumSet<Flag> EMPTY = EnumSet.noneOf(Flag.class);
    public static final EnumSet<Flag> COMPRESSED = EnumSet.of(Flag.COMPRESSED);

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private final FrameBodyTransformer transformer;
    private final EnumSet<Flag> flags;

    public FrameBodyTransformerTest(ProtocolVersion version, Map<String, String> options, EnumSet<Flag> flags)
    {
        this.transformer = getTransformer(version, options);
        Assume.assumeNotNull(transformer);
        this.flags = flags;
    }

    @Test
    public void serde()
    {
        qt().forAll(Generators.bytes(0, 1024)).checkAssert(orFail(bytes -> {
            ByteBuf read = transformer.transformInbound(transformer.transformOutbound(Unpooled.wrappedBuffer(bytes)), flags);
            Assertions.assertThat(ByteBufUtil.hexDump(read))
                      // position is updated, so rely on original bytes
                      .isEqualTo(ByteBufUtil.hexDump(Unpooled.wrappedBuffer(bytes)));
        }));
    }

    @Parameterized.Parameters(name = "{0}-{1}-{2}")
    public static Collection<Object[]> tests()
    {
        List<Object[]> tests = new ArrayList<>();
        // cie checksum/compression
        tests.add(new Object[]{ ProtocolVersion.V4, ImmutableMap.of(USE_LEGACY_CHECKSUMS, "this is ignored", COMPRESSION, "lz4"), EnumSet.of(Flag.COMPRESSED, Flag.V4_CHECKSUMMED) });
        // the specific frame does not have checksum
        tests.add(new Object[]{ ProtocolVersion.V4, ImmutableMap.of(USE_LEGACY_CHECKSUMS, "this is ignored", COMPRESSION, "lz4"), COMPRESSED });
        // if compression isn't also set then this no-ops; test should be skipped by junit
        tests.add(new Object[]{ ProtocolVersion.V4, ImmutableMap.of(USE_LEGACY_CHECKSUMS, "this is ignored"), EMPTY });
        for (ProtocolVersion version : ProtocolVersion.values())
        {
            if (version.supportsChecksums())
            {
                for (ChecksumType ct : ChecksumType.values())
                    tests.add(new Object[]{ version, ImmutableMap.of(CHECKSUM, ct.name()), EnumSet.of(CHECKSUMMED) });
            }
            for (String compressor : Arrays.asList("snappy", "lz4"))
                tests.add(new Object[]{ version, ImmutableMap.of(COMPRESSION, compressor), COMPRESSED });
        }
        return tests;
    }
}