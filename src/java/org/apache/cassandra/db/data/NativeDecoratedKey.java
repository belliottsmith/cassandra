package org.apache.cassandra.db.data;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativeAllocation;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeDecoratedKey extends NativeAllocation implements DecoratedKey
{
    private final Token token;

    public NativeDecoratedKey(ByteBuffer key, Token token, NativeAllocator allocator, OpOrder.Group writeOp)
    {
        this.token = token;
        allocator.allocate(this, key.remaining(), writeOp);
        internalSetBytes(0, key);
    }

    public ByteBuffer key()
    {
        return internalGetByteBuffer(0, (int) internalSize());
    }

    public Token token()
    {
        return token;
    }

    public boolean isMinimum(IPartitioner partitioner)
    {
        return Impl.isMinimum(partitioner);
    }
    public Kind kind()
    {
        return Impl.kind();
    }
    public boolean isMinimum()
    {
        return Impl.isMinimum();
    }
    public int compareTo(RowPosition that)
    {
        return Impl.compareTo(this, that);
    }
    public int hashCode()
    {
        return Impl.hashCode(this);
    }
    public boolean equals(Object obj)
    {
        return Impl.equals(this, obj);
    }
    public String toString()
    {
        return Impl.toString(this);
    }
}
