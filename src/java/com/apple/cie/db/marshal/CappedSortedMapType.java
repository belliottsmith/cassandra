/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */
package com.apple.cie.db.marshal;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FrozenType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

/**
 * CappedSortedMapType supports storing a size-bounded collection of elements keyed by (reversed) timeuuid.  When
 * the collection exceeds the size bound, the oldest elements are removed.
 *
 * The size bound is either stored with the elements in a supplied as a specially encoded tombstone
 * cell withTimeUUID key value (created by {@literal com.apple.cie.cql3.functions.CappedSortedMapType#capFct}), or read
 * from the system properties in {@literal com.apple.cie.db.marshal.cappedsortedmap.defaultcap}.
 *
 * CappedSortedMapType overrides {@literal wrapCellsBuilder} method to supply a custom cell builder that only
 * passes through the size bound (N) if present, then the N most recent cells.
 *
 * To create, use the custom type syntax
 *
 * <pre>
 * CREATE TABLE tbl(pk int PRIMARY KEY, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)');
 * </pre>
 *
 * Add elements to the CSM using {@literal UPDATE} with {@code now()} as the current key.  The capacity
 * can optionally be supplied using the {@literal cap} function.
 *
 * <pre>
 * UPDATE tbl SET csm[cap(?)] = null, csm[now()] = ? WHERE ...;
 * </pre>
 *
 * Remove elements (before they are dropped from the end of the CSM) by updating their value to {@literal null}.
 * If using per-instance capacity, it is important to supply a custom cap when deleting (otherwise
 * older cap cells can be garbage collected used with TTL).
 *
 * <pre>
 * UPDATE tbl SET csm[cap(?)] = null, csm[?] = null WHERE ...;
 * </pre>
 *
 * @param <E> Element type
 */
public class CappedSortedMapType<E> extends MapType<UUID, E>
{
    final private static AbstractType<UUID> keyType = ReversedType.getInstance(TimeUUIDType.instance);
    final private static boolean cappedMapCellResolutionEnabled = AbstractCappedMapCellsResolver.isCappedMapCellResolutionEnabled();

    public CappedSortedMapType(AbstractType<E> elementType)
    {
        super(keyType, elementType, true);
    }

    private static <T> CappedSortedMapType<T> getCappedSortedMapInstance(AbstractType<T> elements)
    {
        return new CappedSortedMapType<>(elements);
    }

    @SuppressWarnings("unused")
    public static CappedSortedMapType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();

        if (l.size() > 1)
            throw new ConfigurationException("CappedSortedMapType takes exactly one type parameters");

        return getCappedSortedMapInstance(l.get(0));
    }

    public Cells.Builder wrapCellsBuilder(Cells.Builder builder)
    {
        return cappedMapCellResolutionEnabled ? CappedSortedMapCellsResolver.instance.wrapCellsBuilder(builder) : builder;
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
          .append(TypeParser.stringifyTypeParameters(Collections.singletonList(getValuesType()), ignoreFreezing || !isMultiCell()));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }
}
