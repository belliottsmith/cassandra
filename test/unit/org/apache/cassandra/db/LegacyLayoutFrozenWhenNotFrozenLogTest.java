package org.apache.cassandra.db;

import org.apache.cassandra.db.LegacyLayout.ComplexCellWithoutCellPathBehavior;

public class LegacyLayoutFrozenWhenNotFrozenLogTest extends LegacyLayoutFrozenWhenNotFrozen
{
    public LegacyLayoutFrozenWhenNotFrozenLogTest()
    {
        super(ComplexCellWithoutCellPathBehavior.LOG);
    }
}
