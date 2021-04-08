package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.transport.messages.ErrorMessage;

/**
 * Enhances {@link SimpleClient} to add custom logic to send to the server.
 */
public class WrappedSimpleClient extends SimpleClient
{
    public WrappedSimpleClient(String host, int port, int version, EncryptionOptions encryptionOptions)
    {
        super(host, port, version, encryptionOptions);
    }

    public WrappedSimpleClient(String host, int port, EncryptionOptions encryptionOptions)
    {
        super(host, port, encryptionOptions);
    }

    public WrappedSimpleClient(String host, int port, int version)
    {
        super(host, port, version);
    }

    public WrappedSimpleClient(String host, int port)
    {
        super(host, port);
    }

    public Message.Response write(ByteBuf buffer) throws InterruptedException
    {
        lastWriteFuture = channel.writeAndFlush(buffer);
        Message.Response response = responseHandler.responses.take();
        if (response instanceof ErrorMessage && ((ErrorMessage) response).error.code() == ExceptionCode.PROTOCOL_ERROR)
        {
            // protocol errors shutdown the connection, wait for it to close
            connection.channel().closeFuture().awaitUninterruptibly();
        }
        return response;
    }
}
