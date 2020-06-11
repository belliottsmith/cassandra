package org.apache.cassandra.db;

import org.apache.cassandra.db.LegacyLayout.ComplexCellWithoutCellPathBehavior;

public class LegacyLayoutFrozenWhenNotFrozenRejectTest extends LegacyLayoutFrozenWhenNotFrozen
{
    public LegacyLayoutFrozenWhenNotFrozenRejectTest()
    {
        super(ComplexCellWithoutCellPathBehavior.REJECT);
    }
}
