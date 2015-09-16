package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputStreamPlus extends DataInputPlus
{
    final InputStream in;
    public DataInputStreamPlus(InputStream in, int bufferSize)
    {
        super(ByteBuffer.allocate(bufferSize));
        buffer.limit(0);
        this.in = in;
    }

    protected void reBuffer() throws IOException
    {
        buffer.clear();
        int count = Math.min(in.available(), buffer.capacity());
        if (count == 0)
        {
            int v = in.read();
            if (v >= 0)
                buffer.put((byte) v);
            buffer.flip();
        }
        else
        {
            int read = in.read(buffer.array(), 0, count);
            assert read == count;
            buffer.limit(read);
        }
    }

    public int available() throws IOException
    {
        return buffer.remaining() + in.available();
    }
}
