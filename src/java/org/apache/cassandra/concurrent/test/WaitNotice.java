package org.apache.cassandra.concurrent.test;

import java.util.concurrent.TimeUnit;

public interface WaitNotice
{

    public void waitForever() throws InterruptedException;

    public boolean waitUntil(long until) throws InterruptedException;

    public void cancel();

    public boolean valid();

}
