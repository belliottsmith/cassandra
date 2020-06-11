package org.apache.cassandra.db;

import org.apache.cassandra.db.LegacyLayout.ComplexCellWithoutCellPathBehavior;

public class LegacyLayoutFrozenWhenNotFrozenAttemptMigrateTest extends LegacyLayoutFrozenWhenNotFrozen
{
    public LegacyLayoutFrozenWhenNotFrozenAttemptMigrateTest()
    {
        super(ComplexCellWithoutCellPathBehavior.ATTEMPT_MIGRATE);
    }
}
