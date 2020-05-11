package org.apache.cassandra.repair;

public class PreviewRepairConflictWithIncrementalRepairException extends IllegalStateException
{
    public PreviewRepairConflictWithIncrementalRepairException(String s)
    {
        super(s);
    }
}
