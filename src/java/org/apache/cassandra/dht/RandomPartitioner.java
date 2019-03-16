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
package org.apache.cassandra.dht;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

/**
 * This class generates a BigIntegerToken using MD5 hash.
 */
public class RandomPartitioner implements IPartitioner
{
    public static final BigInteger ZERO = new BigInteger("0");
    public static final BigIntegerToken MINIMUM = new BigIntegerToken("-1");
    public static final BigInteger MAXIMUM = new BigInteger("2").pow(127);
    public static final int MAXIMUM_TOKEN_SIZE = MAXIMUM.bitLength() / 8 + 1;

    /**
     * Maintain a separate threadlocal message digest, exclusively for token hashing. This is necessary because
     * when Tracing is enabled and using the default tracing implementation, creating the mutations for the trace
     * events involves tokenizing the partition keys. This happens multiple times whilst servicing a ReadCommand,
     * and so can interfere with the stateful digest calculation if the node is a replica producing a digest response.
     */
    private static final ThreadLocal<MessageDigest> localMD5Digest = new ThreadLocal<MessageDigest>()
    {
        @Override
        protected MessageDigest initialValue()
        {
            return FBUtilities.newMessageDigest("MD5");
        }

        @Override
        public MessageDigest get()
        {
            MessageDigest digest = super.get();
            digest.reset();
            return digest;
        }
    };

    private static final int HEAP_SIZE = (int) ObjectSizes.measureDeep(new BigIntegerToken(hashToBigInteger(ByteBuffer.allocate(1))));

    public static final RandomPartitioner instance = new RandomPartitioner();
    public static final AbstractType<?> partitionOrdering = new PartitionerDefinedOrder(instance);


    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new CachedHashDecoratedKey(getToken(key), key);
    }

    public Token midpoint(Token ltoken, Token rtoken)
    {
        // the symbolic MINIMUM token should act as ZERO: the empty bit array
        BigInteger left = ltoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)ltoken).token;
        BigInteger right = rtoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)rtoken).token;
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 127);
        // discard the remainder
        return new BigIntegerToken(midpair.left);
    }

    public BigIntegerToken getMinimumToken()
    {
        return MINIMUM;
    }

    public BigIntegerToken getRandomToken()
    {
        BigInteger token = hashToBigInteger(GuidGenerator.guidAsBytes());
        if ( token.signum() == -1 )
            token = token.multiply(BigInteger.valueOf(-1L));
        return new BigIntegerToken(token);
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
        public ByteBuffer toByteArray(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return ByteBuffer.wrap(bigIntegerToken.token.toByteArray());
        }

        @Override
        public void serialize(Token token, DataOutputPlus out) throws IOException
        {
            out.write(((BigIntegerToken) token).token.toByteArray());
        }

        @Override
        public void serialize(Token token, ByteBuffer out)
        {
            out.put(((BigIntegerToken) token).token.toByteArray());
        }

        @Override
        public int byteSize(Token token)
        {
            return ((BigIntegerToken) token).token.bitLength() / 8 + 1;
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return new BigIntegerToken(new BigInteger(ByteBufferUtil.getArray(bytes)));
        }

        @Override
        public Token fromByteBuffer(ByteBuffer bytes, int position, int length)
        {
            return new BigIntegerToken(new BigInteger(ByteBufferUtil.getArray(bytes, position, length)));
        }

        public String toString(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return bigIntegerToken.token.toString();
        }

        public void validate(String token) throws ConfigurationException
        {
            try
            {
                BigInteger i = new BigInteger(token);
                if (i.compareTo(ZERO) < 0)
                    throw new ConfigurationException("Token must be >= 0");
                if (i.compareTo(MAXIMUM) > 0)
                    throw new ConfigurationException("Token must be <= 2**127");
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(e.getMessage());
            }
        }

        public Token fromString(String string)
        {
            return new BigIntegerToken(new BigInteger(string));
        }
    };

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return false;
    }

    public static class BigIntegerToken extends ComparableObjectToken<BigInteger>
    {
        static final long serialVersionUID = -5833589141319293006L;

        public BigIntegerToken(BigInteger token)
        {
            super(token);
        }

        // convenience method for testing
        @VisibleForTesting
        public BigIntegerToken(String token) {
            this(new BigInteger(token));
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return instance;
        }

        @Override
        public long getHeapSize()
        {
            return HEAP_SIZE;
        }
    }

    public BigIntegerToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;

        return new BigIntegerToken(hashToBigInteger(key));
    }

    public int getMaxTokenSize()
    {
        return MAXIMUM_TOKEN_SIZE;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        Map<Token, Float> ownerships = new HashMap<Token, Float>();
        Iterator<Token> i = sortedTokens.iterator();

        // 0-case
        if (!i.hasNext()) { throw new RuntimeException("No nodes present in the cluster. Has this node finished starting up?"); }
        // 1-case
        if (sortedTokens.size() == 1) {
            ownerships.put(i.next(), new Float(1.0));
        }
        // n-case
        else {
            // NOTE: All divisions must take place in BigDecimals, and all modulo operators must take place in BigIntegers.
            final BigInteger ri = MAXIMUM;                                                  //  (used for addition later)
            final BigDecimal r  = new BigDecimal(ri);                                       // The entire range, 2**127
            Token start = i.next(); BigInteger ti = ((BigIntegerToken)start).token;  // The first token and its value
            Token t; BigInteger tim1 = ti;                                                  // The last token and its value (after loop)
            while (i.hasNext()) {
                t = i.next(); ti = ((BigIntegerToken)t).token;                                      // The next token and its value
                float x = new BigDecimal(ti.subtract(tim1).add(ri).mod(ri)).divide(r).floatValue(); // %age = ((T(i) - T(i-1) + R) % R) / R
                ownerships.put(t, x);                                                               // save (T(i) -> %age)
                tim1 = ti;                                                                          // -> advance loop
            }
            // The start token's range extends backward to the last token, which is why both were saved above.
            float x = new BigDecimal(((BigIntegerToken)start).token.subtract(ti).add(ri).mod(ri)).divide(r).floatValue();
            ownerships.put(start, x);
        }
        return ownerships;
    }

    public AbstractType<?> getTokenValidator()
    {
        return IntegerType.instance;
    }

    public AbstractType<?> partitionOrdering()
    {
        return partitionOrdering;
    }

    private static BigInteger hashToBigInteger(ByteBuffer data)
    {
        MessageDigest messageDigest = localMD5Digest.get();
        if (data.hasArray())
            messageDigest.update(data.array(), data.arrayOffset() + data.position(), data.remaining());
        else
            messageDigest.update(data.duplicate());

        return new BigInteger(messageDigest.digest()).abs();
    }
}
