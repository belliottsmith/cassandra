package org.apache.cassandra.concurrent.test.bench;

import java.util.HashMap;
import java.util.Map;

final class ThreadingMeasurements
{
    final Map<Integer, IntervalTypeMeasurements> map = new HashMap<>();

    IntervalTypeMeasurements get(int threads)
    {
        if (!map.containsKey(threads))
            map.put(threads, new IntervalTypeMeasurements());
        return map.get(threads);
    }

    int removeOutliers()
    {
        int r = 0;
        for (IntervalTypeMeasurements m : map.values())
            r += m.removeOutliers();
        return r;
    }
}


