package org.apache.cassandra.net;

import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.CassandraGenerators.MESSAGE_GEN;
import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

public class MessageOutTest
{
    /**
     * Validates that {@link MessageOut#serializedSize(int)} == {@link MessageOut#serialize(DataOutputPlus, int)} size.
     */
    @Test
    public void serializeSizeProperty()
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            qt().forAll(MESSAGE_GEN).checkAssert(orFail(message -> {
                for (MessagingService.Version version : MessagingService.Version.values())
                {
                    out.clear();
                    message.serialize(out, version.value);
                    Assertions.assertThat(out.getLength())
                              .as("Property serialize(out, version).length == serializedSize(version) " +
                                  "was violated for version %s and verb %s",
                                  version, message.verb)
                              .isEqualTo(message.serializedSize(version.value));
                }
            }));
        }
    }

    /**
     * Table metadata has checks to make sure no metadata is created that is invalid, so make sure those checks pass
     */
    @Test
    public void tables()
    {
        qt().forAll(CassandraGenerators.TABLE_METADATA_GEN).check(ignore -> true);
    }
}