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
package com.apple.cie.db.marshal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.FrozenType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

/**
 * CappedCoalescingMapType supports storing a size-bounded collection of elements keyed by (reversed) timeuuid.  When
 * the collection exceeds the size bound, the oldest elements are removed.
 *
 * The size bound is either stored with the elements in a supplied as a specially encoded tombstone
 * cell withTimeUUID key value (created by {@literal com.apple.cie.cql3.functions.CappedCoalescingMapType#capFct}), or read
 * from the system properties in {@literal com.apple.cie.db.marshal.cappedcoalescingmap.defaultcap}.
 *
 * CappedCoalescingMapType overrides {@literal wrapCellsBuilder} method to supply a custom cell builder that only
 * passes through the size bound (N) if present, then the N most recent cells, AND extracts a coalescing key
 * from the first element of the value tuple and ensures that only the most recent cell for each unique coalescing
 * key is output.
 *
 * To create, use the custom type syntax
 *
 * <pre>
 * CREATE TABLE tbl(pk int PRIMARY KEY, ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)');
 * </pre>
 *
 * Add elements to the CCM using {@literal UPDATE} with {@code now()} as the current key.  The capacity
 * can optionally be supplied using the {@literal cap} function.
 *
 * <pre>
 * UPDATE tbl SET ccm[cap(:capacity)] = null, ccm[now()] = (:coalescingKey, :value) WHERE ...;
 * </pre>
 *
 * Remove elements (before they are dropped from the end of the CCM) by updating their value to {@literal null}.
 * If using per-instance capacity, it is important to supply a custom cap when deleting (otherwise
 * older cap cells can be garbage collected used with TTL).
 *
 * <pre>
 * UPDATE tbl SET ccm[cap(:capacity)] = null, ccm[:key] = null WHERE ...;
 * </pre>
 *
 * @param <E> Element type
 */
public class CappedCoalescingMapType<E> extends CappedSortedMapType<ByteBuffer>
{
    final private AbstractType<E> elementType;
    final private static boolean cappedMapCellResolutionEnabled = AbstractCappedMapCellsResolver.isCappedMapCellResolutionEnabled();

    public CappedCoalescingMapType(AbstractType<E> elementType)
    {
        super(new TupleType(Arrays.asList(BytesType.instance, elementType)));
        this.elementType = elementType;
    }

    private static <T> CappedCoalescingMapType<T> getCappedCoalescingMapInstance(AbstractType<T> elements)
    {
        return new CappedCoalescingMapType<>(elements);
    }

    @SuppressWarnings("unused")
    public static CappedCoalescingMapType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();

        if (l.size() > 1)
            throw new ConfigurationException("CappedCoalescingMapType takes exactly one type parameters");

        return getCappedCoalescingMapInstance(l.get(0));
    }

    public Cells.Builder wrapCellsBuilder(Cells.Builder builder)
    {
        return cappedMapCellResolutionEnabled ? CappedCoalescingMapCellsResolver.instance.wrapCellsBuilder(builder, (TupleType) getValuesType()) : builder;
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
        {
            sb.append(FrozenType.class.getName())
              .append('(');
        }
        sb.append(getClass().getName())
          .append(TypeParser.stringifyTypeParameters(Collections.singletonList(elementType), ignoreFreezing || !isMultiCell()));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }
}
