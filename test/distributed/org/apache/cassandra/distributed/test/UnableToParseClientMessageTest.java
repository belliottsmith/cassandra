package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import io.netty.buffer.Unpooled;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.transport.WrappedSimpleClient;
import org.assertj.core.api.Assertions;

/**
 * If a client sends a message that can not be parsed by the server then we need to detect this and update metrics
 * for monitoring.
 *
 * In rdar://51713271 the issue was a mixed-mode serialization issue between 2.1 and 3.0 with regard to paging.  Since
 * this is a serialization issue we hit similar paths by sending bad bytes to the server, so can simulate the mixed-mode
 * paging issue without needing to send proper messages.
 *
 * @see <a href="rdar://51713271 (Failure to execute queries should emit a KPI other than read timeout/unavailable so it can be alerted/tracked)">rdar://51713271</a>
 */
public class UnableToParseClientMessageTest extends TestBaseImpl
{
    @Test
    public void badMessageCausesProtocolException() throws IOException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.values())).start()))
        {
            // write gibberish to the native protocol
            IInvokableInstance node = cluster.get(1);
            // make sure everything is fine at the start
            node.runOnInstance(() -> {
                Assertions.assertThat(CassandraMetricsRegistry.Metrics.getMeters()
                                                                      .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                      .getCount())
                          .isEqualTo(0);
                Assertions.assertThat(CassandraMetricsRegistry.Metrics.getMeters()
                                                                      .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                      .getCount())
                          .isEqualTo(0);
            });

            try (WrappedSimpleClient client = new WrappedSimpleClient("127.0.0.1", 9042))
            {
                client.connect(false, true);

                // this should return a failed response
                Assertions.assertThat(client.write(Unpooled.wrappedBuffer("This is just a test".getBytes(StandardCharsets.UTF_8))).toString())
                          .contains("Invalid or unsupported protocol version (84); the lowest supported version is 3 and the greatest is 4");

                node.runOnInstance(() -> {
                    // channelRead throws then channelInactive throws after trying to read remaining bytes
                    Assertions.assertThat(CassandraMetricsRegistry.Metrics.getMeters()
                                                                          .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                          .getCount())
                              .isEqualTo(2);
                    Assertions.assertThat(CassandraMetricsRegistry.Metrics.getMeters()
                                                                          .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                          .getCount())
                              .isEqualTo(0);
                });
                Assertions.assertThat(node.logs().grep("Protocol exception with client networking").getResult())
                          .as("Expected logs to contain: Invalid or unsupported protocol version (84)")
                          .allMatch(s -> s.contains("Invalid or unsupported protocol version (84)"))
                          .as(null).hasSize(1); // this logs less offtan than metrics as the log has a nospamlogger wrapper
            }
        }
    }
}
