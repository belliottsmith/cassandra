package org.apache.cassandra.utils.memory;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class NativeAllocation extends AbstractMemory
{

    // parent is either an OffHeapRegion, or it's MarkKey; the latter if it has been freed
    volatile Object parent;
    // NOTE: if and when we move peer references off-heap, they need to be aligned to ensure atomicity even on x86
    private volatile long peer;

    protected final long internalPeer()
    {
        return peer + 4;
    }

    protected final long internalSize()
    {
        return getRealSize() - 4;
    }

    int getRealSize()
    {
        if (unaligned)
            return unsafe.getInt(peer);
        else
            return getIntByByte(peer);
    }

    void setRealSize(int size)
    {
        if (unaligned)
            unsafe.putInt(peer, size);
        else
            putIntByByte(peer, size);
    }

    void setPeer(long peer)
    {
        assert this.peer == 0 || peer == 0;
        peerUpdater.lazySet(this, peer);
    }

    private static final AtomicLongFieldUpdater<NativeAllocation> peerUpdater = AtomicLongFieldUpdater.newUpdater(NativeAllocation.class, "peer");
}
