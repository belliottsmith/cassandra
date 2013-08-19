package org.apache.cassandra.concurrent.test.bench;

public interface OpLimiter
{

    public void authorize(int ops) throws InterruptedException;
    public void complete();

}
