package org.apache.cassandra.db.lifecycle;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.junit.Assert;
import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.quicktheories.QuickTheory.qt;

public class LogFileTest
{
    @Test
    public void testTruncateMillis()
    {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime max = now.plus(10, ChronoUnit.YEARS);
        ZonedDateTime min = now.minus(10, ChronoUnit.YEARS);
        Gen<Long> gen = SourceDSL.longs().between(min.toInstant().toEpochMilli(), max.toInstant().toEpochMilli());

        qt().forAll(gen).checkAssert(jlong -> {
            Instant inst = Instant.ofEpochMilli(jlong);
            Assert.assertEquals(inst.getEpochSecond() * 1000, LogFile.truncateMillis(jlong));
        });
    }
}