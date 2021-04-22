package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import com.codahale.metrics.Meter;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.Util;
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
                // disable waiting on procol errors as that logic was reverted until we can figure out its 100% safe
                // right now ProtocolException is thrown for fatal and non-fatal issues, so closing the channel
                // on non-fatal issues could cause other issues for the cluster
                Assertions.assertThat(client.write(Unpooled.wrappedBuffer("This is just a test".getBytes(StandardCharsets.UTF_8)), false).toString())
                          .contains("Invalid or unsupported protocol version (84); the lowest supported version is 3 and the greatest is 4");

                node.runOnInstance(() -> {
                    // using spinAssertEquals as the metric is updated AFTER replying back to the client
                    // so there is a race where we check the metric before it gets updated
                    Util.spinAssertEquals(1L, // since we reverted the change to close the socket, the channelInactive case doesn't happen
                                          () -> CassandraMetricsRegistry.Metrics.getMeters()
                                                                                   .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                                   .getCount(),
                                          10);

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

    @Test
    public void badMessageCausesProtocolExceptionFromExcludeListAddress() throws IOException, InterruptedException
    {
        badMessageCausesProtocolExceptionFromExcludeList(Arrays.asList("127.0.0.1"));
    }

    @Test
    public void badMessageCausesProtocolExceptionFromExcludeListSubnet() throws IOException, InterruptedException
    {
        badMessageCausesProtocolExceptionFromExcludeList(Arrays.asList("127.0.0.0/31"));
    }

    private void badMessageCausesProtocolExceptionFromExcludeList(List<String> excludeSubnets) throws IOException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.with(Feature.values()).set("client_error_reporting_exclusions", ImmutableMap.of("subnets", excludeSubnets)))
                                           .start()))
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
                // disable waiting on procol errors as that logic was reverted until we can figure out its 100% safe
                // right now ProtocolException is thrown for fatal and non-fatal issues, so closing the channel
                // on non-fatal issues could cause other issues for the cluster
                Assertions.assertThat(client.write(Unpooled.wrappedBuffer("This is just a test".getBytes(StandardCharsets.UTF_8)), false).toString())
                          .contains("Invalid or unsupported protocol version (84); the lowest supported version is 3 and the greatest is 4");

                // metrics are updated AFTER the message is returned, so need to wait for "some time" to make sure
                // metrics don't happen...
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                node.runOnInstance(() -> {
                    // channelRead throws then channelInactive throws after trying to read remaining bytes
                    Assertions.assertThat(CassandraMetricsRegistry.Metrics.getMeters()
                                                                          .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                          .getCount())
                              .isEqualTo(0);
                    Assertions.assertThat(CassandraMetricsRegistry.Metrics.getMeters()
                                                                          .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                          .getCount())
                              .isEqualTo(0);
                });
                Assertions.assertThat(node.logs().grep("Protocol exception with client networking").getResult())
                          .isEmpty();
            }
        }
    }
}
