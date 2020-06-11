package org.apache.cassandra.db;

import org.apache.cassandra.db.LegacyLayout.ComplexCellWithoutCellPathBehavior;

public class LegacyLayoutInvalidDataForMultiCellCollectionRejectTest extends LegacyLayoutInvalidDataForMultiCellCollection
{
    public LegacyLayoutInvalidDataForMultiCellCollectionRejectTest()
    {
        super(ComplexCellWithoutCellPathBehavior.REJECT);
    }
}
