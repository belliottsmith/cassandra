package org.apache.cassandra.concurrent.test;

public interface WaitQueue
{

    public WaitNotice register();
    public void signalOne();
    public void signalAll();

}