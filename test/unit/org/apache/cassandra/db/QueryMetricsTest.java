package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.EstimatedHistogram;

public class QueryMetricsTest extends CQLTester
{
    @Test
    public void memtableRead() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", "v1", "b1");

        for (SinglePartitionQueryTest test : singlePartitions())
        {
            test.run(() -> {
                // reset metrics
                cfs.metric.sstablesPerRead.getBuckets(true);

                execute(test.query, test.bindings);

                long bucketEstimate = 1;
                // rowFound is ignored for now since row filtering isn't currently supported
                long expectedCount = test.partitionFound ? 1 : 0;
                assertEstimatedCount(bucketEstimate, expectedCount, cfs.getSSTablesPerReadHistogramV3());
                assertEstimatedCount(bucketEstimate, expectedCount, cfs.getRecentSSTablesPerReadHistogramV3());
            });
        }
    }

    @Test
    public void sstableRead() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", "v1", "b1");
        flush();

        for (SinglePartitionQueryTest test : singlePartitions())
        {
            test.run(() -> {
                // reset metrics
                cfs.metric.sstablesPerRead.getBuckets(true);

                execute(test.query, test.bindings);

                long bucketEstimate = 1;
                // rowFound is ignored for now since row filtering isn't currently supported
                long expectedCount = test.partitionFound ? 1 : 0;
                assertEstimatedCount(bucketEstimate, expectedCount, cfs.getSSTablesPerReadHistogramV3());
                assertEstimatedCount(bucketEstimate, expectedCount, cfs.getRecentSSTablesPerReadHistogramV3());
            });
        }
    }

    private static void assertEstimatedCount(long estimate, long expected, long[] buckets)
    {
        assertEstimatedCount(estimate, expected, buckets, false);
    }

    private static void assertEstimatedCount(long estimate, long expected, long[] buckets, boolean considerZeroes)
    {
        long[] bucketOffsets = EstimatedHistogram.newOffsets(buckets.length, considerZeroes);

        int index = Arrays.binarySearch(bucketOffsets, estimate);
        if (index < 0)
        {
            // inexact match, take the first bucket higher than n
            index = -index - 1;
        }

        Assert.assertEquals("Bucket count does not match for bucket " + (bucketOffsets[index]) + " and estimate " + estimate + "; buckets\n" + Arrays.toString(bucketOffsets) + "\n" + Arrays.toString(buckets), expected, buckets[index]);
    }

    public static Collection<SinglePartitionQueryTest> singlePartitions()
    {
        List<SinglePartitionQueryTest> tests = new ArrayList<>();
        tests.add(SinglePartitionQueryTest.of("SELECT * FROM %s WHERE a=?", "v1"));
        tests.add(SinglePartitionQueryTest.ofNoPartition("SELECT * FROM %s WHERE a=?", "v2")); // does not exist; expect 0 since metadata can filter
        tests.add(SinglePartitionQueryTest.of("SELECT * FROM %s WHERE a=? AND b=?", "v1", "b1"));
        tests.add(SinglePartitionQueryTest.ofNoRow("SELECT * FROM %s WHERE a=? AND b=?", "v1", "b1")); // does not exist; sadly this doesn't get filtered out, ideally this should be 0
        tests.add(SinglePartitionQueryTest.of("SELECT COUNT(*) FROM %s WHERE a=?", "v1"));

//        // PartitionRangeReadCommand currently doesn't populate these metrics, so exclude for now
//        tests.add(new Object[]{ "SELECT COUNT(*) FROM %s", new Object[]{  }, 0, 1 });
//        tests.add(new Object[]{ "SELECT * FROM %s WHERE b=? ALLOW FILTERING", new Object[]{ "b1" }, 0, 1 });
        return tests;
    }

    private static final class SinglePartitionQueryTest
    {
        private final String query;
        private final Object[] bindings;
        private final boolean partitionFound;
        private final boolean clusteringFound;

        private SinglePartitionQueryTest(String query, Object[] bindings, boolean partitionFound, boolean clusteringFound)
        {
            this.query = query;
            this.bindings = bindings;
            this.partitionFound = partitionFound;
            this.clusteringFound = clusteringFound;
        }

        public void run(FailingRunnable fn)
        {
            try
            {
                fn.run();
            }
            // rewrite the exception to include the test case details.  Stack traces of this code are not imporant so
            // override to be cleaner
            catch (AssertionError cause)
            {
                AssertionError e =  new AssertionError(cause.getMessage() + "\nquery=" + query + ", bindings=" + Arrays.toString(bindings) + ", partitionFound=" + partitionFound + ", clusteringFound=" + clusteringFound);
                e.setStackTrace(cause.getStackTrace());
                throw e;
            }
            catch (Throwable cause)
            {
                AssertionError e =  new AssertionError(cause.getMessage() + "\nquery=" + query + ", bindings=" + Arrays.toString(bindings) + ", partitionFound=" + partitionFound + ", clusteringFound=" + clusteringFound, cause);
                e.setStackTrace(new StackTraceElement[0]);
                throw e;
            }
        }

        static SinglePartitionQueryTest of(String query, Object... bindings)
        {
            return new SinglePartitionQueryTest(query, bindings, true, true);
        }

        static SinglePartitionQueryTest ofNoRow(String query, Object... bindings)
        {
            return new SinglePartitionQueryTest(query, bindings, true, false);
        }

        static SinglePartitionQueryTest ofNoPartition(String query, Object... bindings)
        {
            return new SinglePartitionQueryTest(query, bindings, false, false);
        }
    }

    interface FailingRunnable
    {
        void run() throws Throwable;
    }
}
