package org.apache.cassandra.utils.memory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class NativeAllocation extends AbstractMemory
{

    // parent is either an OffHeapRegion, or it's MarkKey; the latter if it has been freed
    volatile Object parent;
    // a chain of allocations being served from the same region
    NativeAllocation next;
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

    public final boolean free()
    {
        while (true)
        {
            Object obj = parent;
            if (obj instanceof NativeRegion.MarkKey)
                return false;
            NativeRegion parent = (NativeRegion) obj;
            int size = getRealSize();
            if (!swapParent(parent, parent.markKey))
                continue;
            parent.free(size);
            return true;
        }
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

    boolean swapParent(Object exp, Object upd)
    {
        return parentUpdater.compareAndSet(this, exp, upd);
    }

    void migrate(long to)
    {
        assert peer != 0 && to != 0;
        internalSetBytes(peer, to, getRealSize());
        peerUpdater.lazySet(this, to);
    }

    void setPeer(long peer)
    {
        assert this.peer == 0 || peer == 0;
        peerUpdater.lazySet(this, peer);
    }

    static Iterable<NativeAllocation> iterate(final NativeAllocation head)
    {
        return new Iterable<NativeAllocation>()
        {
            public Iterator<NativeAllocation> iterator()
            {
                return new Iterator<NativeAllocation>()
                {
                    private NativeAllocation next = head;
                    public boolean hasNext()
                    {
                        return next != null;
                    }
                    public NativeAllocation next()
                    {
                        NativeAllocation result = next;
                        next = next.next;
                        return result;
                    }
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private static final AtomicReferenceFieldUpdater<NativeAllocation, Object> parentUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeAllocation.class, Object.class, "parent");
    private static final AtomicLongFieldUpdater<NativeAllocation> peerUpdater = AtomicLongFieldUpdater.newUpdater(NativeAllocation.class, "peer");
}
