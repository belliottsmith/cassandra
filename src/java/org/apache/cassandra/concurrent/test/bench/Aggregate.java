package org.apache.cassandra.concurrent.test.bench;

final class Aggregate
{
    final String opsIn, opsOut, opCost;

    public Aggregate(String opsIn, String opsOut, String opCost)
    {
        this.opsIn = opsIn;
        this.opsOut = opsOut;
        this.opCost = opCost;
    }
}
