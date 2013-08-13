package org.apache.cassandra.concurrent.test;

public interface WaitQueue
{

    public WaitSignal register();
    public void signalOne();
    public void signalAll();

}