/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.transport.frame.FrameBodyTransformer;
import org.apache.cassandra.transport.frame.checksum.ChecksummingTransformer;
import org.apache.cassandra.transport.frame.compress.CompressingTransformer;
import org.apache.cassandra.transport.frame.compress.Compressor;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.transport.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ChecksumType;

/**
 * The initial message of the protocol.
 * Sets up a number of connection options.
 */
public class StartupMessage extends Message.Request
{
    public static final String CQL_VERSION = "CQL_VERSION";
    public static final String COMPRESSION = "COMPRESSION";
    public static final String PROTOCOL_VERSIONS = "PROTOCOL_VERSIONS";
    public static final String DRIVER_NAME = "DRIVER_NAME";
    public static final String DRIVER_VERSION = "DRIVER_VERSION";
    public static final String CHECKSUM = "CONTENT_CHECKSUM";
    public static final String THROW_ON_OVERLOAD = "THROW_ON_OVERLOAD";
    public static final String USE_LEGACY_CHECKSUMS = "USE_CHECKSUMS"; // CIE 2.1/3.0 V4 protocol checksums

    public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>()
    {
        public StartupMessage decode(ByteBuf body, ProtocolVersion version)
        {
            return new StartupMessage(upperCaseKeys(CBUtil.readStringMap(body)));
        }

        public void encode(StartupMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeStringMap(msg.options, dest);
        }

        public int encodedSize(StartupMessage msg, ProtocolVersion version)
        {
            return CBUtil.sizeOfStringMap(msg.options);
        }
    };

    public final Map<String, String> options;

    public StartupMessage(Map<String, String> options)
    {
        super(Message.Type.STARTUP);
        this.options = options;
    }

    @Override
    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
    {
        String cqlVersion = options.get(CQL_VERSION);
        if (cqlVersion == null)
            throw new ProtocolException("Missing value CQL_VERSION in STARTUP message");

        try
        {
            if (new CassandraVersion(cqlVersion).compareTo(new CassandraVersion("2.99.0")) < 0)
                throw new ProtocolException(String.format("CQL version %s is not supported by the binary protocol (supported version are >= 3.0.0)", cqlVersion));
        }
        catch (IllegalArgumentException e)
        {
            throw new ProtocolException(e.getMessage());
        }

        connection.setTransformer(getTransformer(connection.getVersion(), options));
        connection.setThrowOnOverload("1".equals(options.get(THROW_ON_OVERLOAD)));

        ClientState clientState = state.getClientState();
        String driverName = options.get(DRIVER_NAME);
        if (null != driverName)
        {
            clientState.setDriverName(driverName);
            clientState.setDriverVersion(options.get(DRIVER_VERSION));
        }

        if (DatabaseDescriptor.getAuthenticator().requireAuthentication())
            return new AuthenticateMessage(DatabaseDescriptor.getAuthenticator().getClass().getName());
        else
            return new ReadyMessage();
    }

    private static Map<String, String> upperCaseKeys(Map<String, String> options)
    {
        Map<String, String> newMap = new HashMap<String, String>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet())
            newMap.put(entry.getKey().toUpperCase(), entry.getValue());
        return newMap;
    }

    private static ChecksumType getChecksumType(Map<String, String> options) throws ProtocolException
    {
        String name = options.get(CHECKSUM);
        try
        {
            return name != null ? ChecksumType.valueOf(name.toUpperCase()) : null;
        }
        catch (IllegalArgumentException e)
        {
            throw new ProtocolException(String.format("Requested checksum type %s is not known or supported by " +
                                                      "this version of Cassandra", name));
        }
    }

    private static Compressor getCompressor(Map<String, String> options) throws ProtocolException
    {
        String name = options.get(COMPRESSION);
        if (null == name)
            return null;

        switch (name.toLowerCase())
        {
            case "snappy":
            {
                if (SnappyCompressor.INSTANCE == null)
                    throw new ProtocolException("This instance does not support Snappy compression");

                return SnappyCompressor.INSTANCE;
            }
            case "lz4":
                return LZ4Compressor.INSTANCE;
            default:
                throw new ProtocolException(String.format("Unknown compression algorithm: %s", name));
        }
    }

    public static FrameBodyTransformer getTransformer(ProtocolVersion version, Map<String, String> options)
    {
        ChecksumType checksumType = getChecksumType(options);
        Compressor compressor = getCompressor(options);

        // Backward compatibility with V4 protocol clients requesting the CIE version of checksums
        // Filter on V4 in case conflicting header flag added over the internal USE_CHECKSUMS.
        boolean useV4Checksums = version.supportsV4Checksums() && options.containsKey(USE_LEGACY_CHECKSUMS) && compressor == LZ4Compressor.INSTANCE;

        if (useV4Checksums)
        {
            return ChecksummingTransformer.getV4ChecksumTransformer();
        }
        else if (null != checksumType)
        {
            if (!version.supportsChecksums())
                throw new ProtocolException(String.format("Invalid message flag. Protocol version %s does not support frame body checksums", version.toString()));
            return ChecksummingTransformer.getTransformer(checksumType, compressor);
        }
        else if (null != compressor)
        {
            return CompressingTransformer.getTransformer(compressor);
        }
        return null;
    }

    @Override
    public String toString()
    {
        return "STARTUP " + options;
    }
}
