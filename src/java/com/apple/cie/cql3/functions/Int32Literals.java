/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */
package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Custom function to supply int32 literal values for the k argument of
 * {@literal com.apple.cie.cql3.functions.Apns#koldestdeliverableFct} as it
 * is not possibly to supply bound parameters in SELECT queries in the 3.0 series.
 *
 * No longer strictly required in CIE4.0 however they are being left here until
 * the APNs team have migrated any of their code to use literals.
 */
public abstract class Int32Literals
{
    private static final int maxLiterals = Integer.getInteger("com.apple.cie.cql3.functions.int32literals.max", 20);

    static String literalName(int value)
    {
        assert value <= maxLiterals : ("Too large.  Set -Dcom.apple.cie.cql3.functions.int32literals.max=" + value);
        return "int32literal" + value;
    }

    static class Int32LiteralFunction extends NativeScalarFunction
    {
        private final int value;

        Int32LiteralFunction(int value)
        {
            super(literalName(value), Int32Type.instance);
            this.value = value;
        }

        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
        {
            return Int32Type.instance.decompose(value);
        }
    }

    @SuppressWarnings("unused")
    public static Collection<Function> all()
    {
        return IntStream.range(0, maxLiterals + 1).mapToObj(Int32LiteralFunction::new).collect(Collectors.toList());
    }
}
