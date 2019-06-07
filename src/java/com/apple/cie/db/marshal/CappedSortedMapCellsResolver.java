/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */
package com.apple.cie.db.marshal;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;

/**
 * Cell resolver for CappedSortedMapType.  Caps the number of cells to the limit provided with the capped map,
 * or the node-wide default.
 */
public class CappedSortedMapCellsResolver extends AbstractCappedMapCellsResolver
{
    private static final String typeName = CappedSortedMapType.class.getSimpleName();
    private static final String MBEAN_NAME = "com.apple.cie.db:type=" + typeName;

    static final CappedSortedMapCellsResolver instance = new CappedSortedMapCellsResolver();

    public CappedSortedMapCellsResolver()
    {
        super(MBEAN_NAME, Integer.getInteger("com.apple.cie.db.marshal.cappedsortedmap.defaultcap", 10));
    }

    Cells.Builder wrapCellsBuilder(Cells.Builder resolver)
    {
        return enabled ? new WrapperBuilder(resolver, instance.defaultCap) : resolver;
    }

    String typeName()
    {
        return typeName;
    }

    private class WrapperBuilder extends AbstractCappedMapCellsResolver.WrapperBuilder
    {
        public WrapperBuilder(Cells.Builder builder, int defaultCap)
        {
            super(builder, defaultCap);
        }

        protected void onRegularCell(Cell cell)
        {
            if (regularsOut < cap)
            {
                keepCell(cell);
            }
        }
    }
}
