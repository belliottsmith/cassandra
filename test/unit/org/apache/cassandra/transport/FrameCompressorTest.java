package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.transport.Frame.Header.Flag;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

@RunWith(Parameterized.class)
public class FrameCompressorTest
{
    private static final EnumSet<Flag> EMPTY = EnumSet.noneOf(Flag.class);

    private final FrameCompressor compressor;
    private final EnumSet<Flag> flags;

    public FrameCompressorTest(FrameCompressor compressor, EnumSet<Flag> flags)
    {
        this.compressor = compressor;
        this.flags = flags;
    }

    @Test
    public void serde()
    {
        qt().forAll(Generators.bytes(0, 1024)).checkAssert(orFail(bytes -> {
            Frame read = compressor.decompress(compressor.compress(toFrame(bytes)));
            Assertions.assertThat(ByteBufUtil.hexDump(read.body))
                      // position is updated, so rely on original bytes
                      .isEqualTo(ByteBufUtil.hexDump(Unpooled.wrappedBuffer(bytes)));
        }));
    }

    private Frame toFrame(ByteBuffer bytes)
    {
        return Frame.create(Message.Type.OPTIONS, 0, Server.CURRENT_VERSION, flags, Unpooled.wrappedBuffer(bytes));
    }

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> tests() {
        return Arrays.asList(new Object[][] {
        { FrameCompressor.LZ4Compressor.instance, EMPTY },
        { FrameCompressor.LZ4Compressor.instance, EnumSet.of(Flag.SUPPORTS_LZ4_BLOCK_FORMAT_WITH_CHECKSUM) },
        { FrameCompressor.SnappyCompressor.instance, EMPTY }
        });
    }
}