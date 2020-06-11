package org.apache.cassandra.db;

import org.apache.cassandra.db.LegacyLayout.ComplexCellWithoutCellPathBehavior;

public class LegacyLayoutInvalidDataForMultiCellCollectionLogTest extends LegacyLayoutInvalidDataForMultiCellCollection
{
    public LegacyLayoutInvalidDataForMultiCellCollectionLogTest()
    {
        super(ComplexCellWithoutCellPathBehavior.LOG);
    }
}
