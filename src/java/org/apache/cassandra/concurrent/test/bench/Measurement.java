package org.apache.cassandra.concurrent.test.bench;

final class Measurement
{

    final long opsIn, opsOut, elapsedCpuNanos, elapsedMillis;

    public Measurement(long opsIn, long opsOut, long elapsedCpuNanos, long elapsedMillis)
    {
        this.opsIn = opsIn;
        this.opsOut = opsOut;
        this.elapsedCpuNanos = elapsedCpuNanos;
        this.elapsedMillis = elapsedMillis;
    }

    public Measurement(double opsInRate, double opsOutRate, double cyclesPerOp)
    {
        elapsedMillis = 1000000;
        opsIn = (long) (opsInRate * 1000000000);
        opsOut = (long) (opsOutRate * 1000000000);
        double elapsedCycles = (opsIn + opsOut) * cyclesPerOp;
        elapsedCpuNanos = (long) (elapsedCycles / Benchmark.CLOCK_RATE);
    }

    static double rateMops(long opCount, long elapsedMillis)
    {
        return (opCount / 1000000d) / (elapsedMillis / 1000d);
    }

    double getInRateMops()
    {
        return rateMops(opsIn, elapsedMillis);
    }

    double getOutRateMops()
    {
        return rateMops(opsOut, elapsedMillis);
    }

    static double totalGops(long opCount)
    {
        return opCount / 1000000000d;
    }

    double getTotalGOpsIn()
    {
        return opsIn / 1000000000d;
    }

    double getTotalGOpsOut()
    {
        return opsOut / 1000000000d;
    }

    double getTotalRateMops()
    {
        return rateMops(opsOut + opsIn, elapsedMillis);
    }

    double cycles()
    {
        return elapsedCpuNanos * Benchmark.CLOCK_RATE;
    }

    double cyclesPerOp()
    {
        return cycles() / (opsIn + opsOut);
    }

    double elapsedSeconds()
    {
        return elapsedMillis / 1000d;
    }

    double elapsedCpuSeconds()
    {
        return elapsedCpuNanos / 1000000000d;
    }

    void print(Test test, MeasurementIntervalType type)
    {
        String formatstr;
        switch (type)
        {
            case TOTAL:
                formatstr = "%s: total elapsed %.2fs (%.2fcpu), averaging %.2f Mop/s, at %.2f cycles/op";
                System.out.println(String.format(formatstr, test, elapsedSeconds(), elapsedCpuSeconds(), getTotalRateMops(), cyclesPerOp()));
                break;
            case INTERVAL:
                formatstr = "%s: elapsed %.2fs, averaging %.2f Mop/s in, %.2f Mop/s out, at %.2f cycles/op";
                System.out.println(String.format(formatstr, test, elapsedSeconds(), getInRateMops(), getOutRateMops(), cyclesPerOp()));
                break;
        }
    }

    double measurement(MeasurementType type)
    {
        switch (type)
        {
            case CYCLES_PER_OP:
                return cyclesPerOp();
            case OP_RATE_IN:
                return getInRateMops();
            case OP_RATE_OUT:
                return getOutRateMops();
        }
        throw new IllegalStateException();
    }

}
