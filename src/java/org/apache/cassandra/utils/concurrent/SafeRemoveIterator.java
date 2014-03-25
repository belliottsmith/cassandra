package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;

public interface SafeRemoveIterator<V> extends Iterator<V>
{

    /**
     * Like remove(), except executes atomically, returning success/failure
     *
     * @return true iff we atomically removed the item, false if it was removed by another thread
     */
    public boolean safeRemove();

}

