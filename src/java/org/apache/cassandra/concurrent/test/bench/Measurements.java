package org.apache.cassandra.concurrent.test.bench;

import java.util.EnumMap;
import java.util.Map;

final class Measurements
{
    final EnumMap<Test, ThreadingMeasurements> map = new EnumMap<>(Test.class);

    ThreadingMeasurements get(Test test)
    {
        if (!map.containsKey(test))
            map.put(test, new ThreadingMeasurements());
        return map.get(test);
    }

    void print(boolean groupResults)
    {
        printResults(getResults(groupResults));
    }

    String[][] getResults(boolean groupTests)
    {
        final String[][] results = new String[2 + (Benchmark.THREADS.length * 3)][2 + (map.size() * AggregateType.values().length)];
        addHeaders(results);
        int row = 2;
        if (groupTests)
            for (Test test : map.keySet())
                for (AggregateType type : AggregateType.values())
                    addResult(results, row++, test, type);
        else
            for (AggregateType type : AggregateType.values())
                for (Test test : map.keySet())
                    addResult(results, row++, test, type);

        return results;
    }

    void addHeaders(String[][] results)
    {
        results[0][1] = "Queue";
        results[0][2] = "Stat";
        for (int i = 0; i < Benchmark.THREADS.length; i++)
        {
            int offset = 2 + (i * 3);
            results[0 + offset][0] = Benchmark.THREADS[i] + " Threads";
            results[1 + offset][0] = Benchmark.THREADS[i] + " Threads";
            results[0 + offset][1] = "Ops In";
            results[1 + offset][1] = "Ops Out";
            results[2 + offset][1] = "Op Cost";
        }
    }

    void addResult(String[][] results, int row, Test test, AggregateType type)
    {
        results[0][row] = test.name();
        results[1][row] = type.name().toLowerCase();
        for (int i = 0; i < Benchmark.THREADS.length; i++)
        {
            Aggregate aggregate = map.get(test).get(Benchmark.THREADS[i]).aggregate(type);
            int offset = 2 + (i * 3);
            results[0 + offset][row] = aggregate.opsIn;
            results[1 + offset][row] = aggregate.opsOut;
            results[2 + offset][row] = aggregate.opCost;
        }
    }

    void printResults(String[][] results)
    {
        final String[] formatstrs = new String[results.length];
        for (int c = 0; c != results.length; c++)
        {
            int maxLen = 0;
            String[] column = results[c];
            for (int r = 0; r < column.length; r++)
            {
                String str = column[r];
                if (str == null)
                    continue;
                maxLen = Math.max(maxLen, str.length());
            }
            formatstrs[c] = "%" + (maxLen + 1) + "s";
        }
        for (int r = 0; r < results[0].length; r++)
        {
            for (int c = 0; c != results.length; c++)
                print(formatstrs[c], results[c][r]);
            System.out.println();
        }
    }

    private static void print(String formatstr, String str)
    {
        if (str == null)
            str = "";
        System.out.print(String.format(formatstr, str));
        System.out.print(" ");
    }

    void removeOutliers()
    {
        for (Map.Entry<Test, ThreadingMeasurements> e : map.entrySet())
        {
            int removed = e.getValue().removeOutliers();
            if (removed != 0)
                System.out.println(String.format("Removed %d outliers from %s", removed, e.getKey()));
        }
    }
}

