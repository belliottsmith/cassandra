package org.apache.cassandra.concurrent.test.bench;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;

final class IntervalTypeMeasurements
{
    final EnumMap<MeasurementIntervalType, List<Measurement>> map = new EnumMap<>(MeasurementIntervalType.class);

    void add(MeasurementIntervalType intervalType, Measurement measurement)
    {
        if (!map.containsKey(intervalType))
            map.put(intervalType, new ArrayList<Measurement>());
        map.get(intervalType).add(measurement);
    }

    Aggregate aggregate(AggregateType type)
    {
        switch (type)
        {
            case AVERAGE:
                return average(MeasurementIntervalType.INTERVAL);
            case STDEV:
                return stdev(MeasurementIntervalType.INTERVAL);
            case PEAK:
                return peak(MeasurementIntervalType.INTERVAL);
            case PRE_TIDY:
                return total(MeasurementIntervalType.DONE_PUTS);
            case TOTAL:
                return total(MeasurementIntervalType.TOTAL);
        }
        throw new IllegalStateException();
    }

    long totalElapsedMillis(MeasurementIntervalType intervalType)
    {
        if (!map.containsKey(intervalType))
            return 0;
        long elapsed = 0;
        for (Measurement m : map.get(intervalType))
            elapsed += m.elapsedMillis;
        return elapsed;
    }

    Aggregate total(MeasurementIntervalType intervalType)
    {
        final Measurement total = total(map.get(intervalType));
        final String gigOpsIn = String.format("%.2f GOps ", total.getTotalGOpsIn());
        final String gigOpsOut = String.format("%.2f GOps ", total.getTotalGOpsOut());
        final String opCost = String.format("%.2f cycles", total.cyclesPerOp());
        return new Aggregate(gigOpsIn, gigOpsOut, opCost);
    }

    Aggregate average(MeasurementIntervalType intervalType)
    {
        final Measurement total = total(map.get(intervalType));
        final String opsInRate = String.format("%.2f MOp/s", total.getInRateMops());
        final String opsOutRate = String.format("%.2f MOp/s", total.getOutRateMops());
        final String opCost = String.format("%.2f cycles", total.cyclesPerOp());
        return new Aggregate(opsInRate, opsOutRate, opCost);
    }

    Aggregate stdev(MeasurementIntervalType intervalType)
    {
        final Measurement total = stdev(map.get(intervalType));
        final String opsInRate = String.format("%.2f MOp/s", total.getInRateMops());
        final String opsOutRate = String.format("%.2f MOp/s", total.getOutRateMops());
        final String opCost = String.format("%.2f cycles", total.cyclesPerOp());
        return new Aggregate(opsInRate, opsOutRate, opCost);
    }

    Measurement total(List<Measurement> measurements)
    {
        long opsIn = 0, opsOut = 0, elapsedCpuNanos = 0, elapsedMillis = 0;
        for (Measurement m : measurements)
        {
            opsIn += m.opsIn;
            opsOut += m.opsOut;
            elapsedCpuNanos += m.elapsedCpuNanos;
            elapsedMillis += m.elapsedMillis;
        }
        return new Measurement(opsIn, opsOut, elapsedCpuNanos, elapsedMillis);
    }

    Measurement stdev(List<Measurement> measurements)
    {
        double opsIn = 0, opsOut = 0, cyclesPerOp = 0;
        double opsInSq = 0, opsOutSq = 0, cyclesPerOpSq = 0;
        int count = 0;
        for (Measurement m : measurements)
        {
            double in = m.getInRateMops();
            double out = m.getOutRateMops();
            double cyc = m.cyclesPerOp();
            opsIn += in;
            opsOut += out;
            cyclesPerOp += cyc;
            opsInSq += in * in;
            opsOutSq += out * out;
            cyclesPerOpSq += cyc * cyc;
            count++;
        }
        return new Measurement(
                stdev(opsIn, opsInSq, count),
                stdev(opsOut, opsOutSq, count),
                stdev(cyclesPerOp, cyclesPerOpSq, count));
    }

    static double stdev(double sum, double sumsq, int count)
    {
        final long mean = (long) Math.round(sum / count);
        return Math.sqrt((sumsq / count) - (mean * (double) mean));
    }

    int removeOutliers()
    {
        int removed = 0;
        List<Measurement> measurements = map.get(MeasurementIntervalType.INTERVAL);
        {
            Measurement stdev = stdev(measurements);
            Measurement total = total(measurements);

            double rangeOpsIn = 3 * stdev.getInRateMops();
            double rangeOpsOut = 3 * stdev.getOutRateMops();
            double rangeCyclesPerOp = 3 * stdev.cyclesPerOp();
            double meanOpsIn = total.getInRateMops();
            double meanOpsOut = total.getInRateMops();
            double meanCyclesPerOp = total.cyclesPerOp();

            final Iterator<Measurement> iter = measurements.iterator();
            int initial = measurements.size();
            int kept = 0;
            while (iter.hasNext())
            {
                Measurement m = iter.next();
                if (!inRange(m.getInRateMops(), meanOpsIn, rangeOpsIn))
                    iter.remove();
                else if (!inRange(m.getOutRateMops(), meanOpsOut, rangeOpsOut))
                    iter.remove();
                else if (!inRange(m.cyclesPerOp(), meanCyclesPerOp, rangeCyclesPerOp))
                    iter.remove();
                kept++;
            }
            removed += initial - kept;
        }
        return removed;
    }

    static boolean inRange(double v, double mean, double range)
    {
        return Double.isNaN(range) || Math.abs(v - mean) <= range;
    }

    Aggregate peak(MeasurementIntervalType intervalType)
    {
        List<Measurement> measurements = map.get(intervalType);
        double opsIn = 0, opsOut = 0, cyclesPerOp = Double.MAX_VALUE;
        for (Measurement m : measurements)
        {
            if (m.getInRateMops() > opsIn)
                opsIn = m.getInRateMops();
            if (m.getOutRateMops() > opsOut)
                opsOut = m.getOutRateMops();
            if (m.cyclesPerOp() < cyclesPerOp)
                cyclesPerOp = m.cyclesPerOp();
        }
        final String opsInRate = String.format("%.2f MOp/s", opsIn);
        final String opsOutRate = String.format("%.2f MOp/s", opsOut);
        final String opCost = String.format("%.2f cycles", cyclesPerOp);
        return new Aggregate(opsInRate, opsOutRate, opCost);
    }

}
