package org.apache.cassandra.metrics;

import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 *
 */
public class BlacklistMetrics
{
    private final Meter writesRejected;;
    private final Meter readsRejected;
    private final Meter rangeReadsRejected;
    private final Meter totalRequestsRejected;

    public BlacklistMetrics()
    {
        final MetricNameFactory factory = new DefaultNameFactory("StorageProxy", "PartitionBlacklist");
        writesRejected = Metrics.meter(factory.createMetricName("WriteRejected"));
        readsRejected = Metrics.meter(factory.createMetricName("ReadRejected"));
        rangeReadsRejected = Metrics.meter(factory.createMetricName("RangeReadRejected"));
        totalRequestsRejected = Metrics.meter(factory.createMetricName("TotalRejected"));
    }

    public void incrementWritesRejected()
    {
        writesRejected.mark();
    }

    public void incrementReadsRejected()
    {
        readsRejected.mark();
    }

    public void incrementRangeReadsRejected()
    {
        rangeReadsRejected.mark();
    }

    public void incrementTotalRejected()
    {
        totalRequestsRejected.mark();
    }
}
