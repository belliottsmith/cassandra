package org.apache.cassandra.cql3;

import org.junit.Assert;
import org.junit.Test;

public class NotReservedTest extends CQLTester
{
    @Test
    public void replace() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, replace text);");
        execute("INSERT INTO %s (id, replace) VALUES ('a', 'b')");
        UntypedResultSet result = execute("SELECT id, replace FROM %s WHERE id='a'");
        UntypedResultSet.Row row = result.one();
        Assert.assertNotNull(row);
        Assert.assertEquals("b", row.getString("replace"));
    }
}
