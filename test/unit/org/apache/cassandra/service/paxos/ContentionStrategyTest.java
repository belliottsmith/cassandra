/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.paxos;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.service.paxos.ContentionStrategy.Bound;
import org.apache.cassandra.service.paxos.ContentionStrategy.ParsedStrategy;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.paxos.ContentionStrategy.LatencyModifier;
import org.apache.cassandra.service.paxos.ContentionStrategy.LatencySelector;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.DoubleToLongFunction;

import static org.apache.cassandra.service.paxos.ContentionStrategy.*;
import static org.apache.cassandra.service.paxos.ContentionStrategy.LatencyModifier.*;
import static org.apache.cassandra.service.paxos.ContentionStrategy.LatencySelector.*;

public class ContentionStrategyTest
{

    static
    {
        DatabaseDescriptor.forceStaticInitialization();
    }

    private static final long MAX = maxQueryTimeout()/2;

    private static final BoundParseValidator DEFAULT_MIN_VALIDATOR = new BoundParseValidator(defaultMinWait(), true, assertBound(0, MAX, 0, maxReadWrite(0f).getClass(), 0.50, 0, multiply(0f).getClass(), 0.66));
    private static final BoundParseValidator DEFAULT_MAX_VALIDATOR = new BoundParseValidator(defaultMaxWait(), false, assertBound(10000, 100000, 100000, maxReadWrite(0f).getClass(), 0.95, 0, multiplyByAttemptsExp(0f).getClass(), 1.8));
    private static final BoundParseValidator DEFAULT_MIN_DELTA_VALIDATOR = new BoundParseValidator(defaultMinDelta(), true, assertBound(5000, MAX, 5000, maxReadWrite(0f).getClass(), 0.50, 0, multiply(0f).getClass(), 0.5));

    private static List<BoundParseValidator> VALIDATE = ImmutableList.of(
            new BoundParseValidator("p95(rw)", false, assertBound(0, MAX, MAX, maxReadWrite(0f).getClass(), 0.95, 0, identity().getClass(), 1)),
            new BoundParseValidator("5ms<=p50(rw)*0.66", false, assertBound(5000, MAX, MAX, maxReadWrite(0f).getClass(), 0.50, 0, multiply(0).getClass(), 0.66)),
            new BoundParseValidator("5us <= p50(r)*1.66*attempts", true, assertBound(5, MAX, 5, read(0f).getClass(), 0.50, 0, multiplyByAttempts(0f).getClass(), 1.66)),
            new BoundParseValidator("0<=p50(w)*0.66^attempts", true, assertBound(0, MAX, 0, write(0f).getClass(), 0.50, 0, multiplyByAttemptsExp(0f).getClass(), 0.66)),
            new BoundParseValidator("125us", true, assertBound(125, 125, 125, constant(0).getClass(), 0.0f, 125, identity().getClass(), 1)),
            new BoundParseValidator("5us <= p95(r)*1.8^attempts <= 100us", true, assertBound(5, 100, 5, read(0f).getClass(), 0.95, 0, multiplyByAttemptsExp(0f).getClass(), 1.8)),
            DEFAULT_MIN_VALIDATOR, DEFAULT_MAX_VALIDATOR, DEFAULT_MIN_DELTA_VALIDATOR
    );

    static class BoundParseValidator
    {
        final String spec;
        final boolean isMin;
        final Consumer<Bound> validator;

        BoundParseValidator(String spec, boolean isMin, Consumer<Bound> validator)
        {
            this.spec = spec;
            this.isMin = isMin;
            this.validator = validator;
        }

        void validate(Bound bound)
        {
            validator.accept(bound);
        }
    }

    private static Consumer<Bound> assertBound(
                             long min, long max, long onFailure,
                             Class<? extends LatencySelector> selectorClass,
                             double selectorPercentile,
                             long selectorConst,
                             Class<? extends LatencyModifier> modifierClass,
                             double modifierVal
    )
    {
        return bound -> {
            Assert.assertEquals(min, bound.min);
            Assert.assertEquals(max, bound.max);
            Assert.assertEquals(onFailure, bound.onFailure);
            Assert.assertSame(selectorClass, bound.selector.getClass());
            if (selectorClass == constant(0).getClass())
            {
                DoubleToLongFunction fail = d -> { throw new AssertionError(); };
                Assert.assertEquals(selectorConst, bound.selector.select(fail, fail));
            }
            else
            {
                AtomicReference<Double> percentile = new AtomicReference<>();
                DoubleToLongFunction set = d -> { percentile.set(d); return 0L; };
                bound.selector.select(set, set);
                Assert.assertNotNull(percentile.get());
                Assert.assertEquals(selectorPercentile, percentile.get(), 0.00001);
            }
            Assert.assertSame(modifierClass, bound.modifier.getClass());
            Assert.assertEquals(1000000L * modifierVal, bound.modifier.modify(1000000, 1), 0.00001);
        };
    }

    private static void assertParseFailure(String spec)
    {

        try
        {
            parseBound(spec, false);
            Assert.fail("expected parse failure");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void strategyParseTest()
    {
        for (BoundParseValidator min : VALIDATE.stream().filter(v -> v.isMin).toArray(BoundParseValidator[]::new))
        {
            for (BoundParseValidator max : VALIDATE.stream().filter(v -> !v.isMin).toArray(BoundParseValidator[]::new))
            {
                for (BoundParseValidator minDelta : VALIDATE.stream().filter(v -> v.isMin).toArray(BoundParseValidator[]::new))
                {
                    {
                        ParsedStrategy parsed = parseStrategy("min=" + min.spec + ",max=" + max.spec + ",delta=" + minDelta.spec);
                        Assert.assertEquals(parsed.min, min.spec);
                        min.validate(parsed.strategy.min);
                        Assert.assertEquals(parsed.max, max.spec);
                        max.validate(parsed.strategy.max);
                        Assert.assertEquals(parsed.minDelta, minDelta.spec);
                        minDelta.validate(parsed.strategy.minDelta);
                    }
                    ParsedStrategy parsed = parseStrategy("delta=" + minDelta.spec);
                    Assert.assertEquals(parsed.min, DEFAULT_MIN_VALIDATOR.spec);
                    DEFAULT_MIN_VALIDATOR.validate(parsed.strategy.min);
                    Assert.assertEquals(parsed.max, DEFAULT_MAX_VALIDATOR.spec);
                    DEFAULT_MAX_VALIDATOR.validate(parsed.strategy.max);
                    Assert.assertEquals(parsed.minDelta, minDelta.spec);
                    minDelta.validate(parsed.strategy.minDelta);
                }
                ParsedStrategy parsed = parseStrategy("max=" + max.spec);
                Assert.assertEquals(parsed.min, DEFAULT_MIN_VALIDATOR.spec);
                DEFAULT_MIN_VALIDATOR.validate(parsed.strategy.min);
                Assert.assertEquals(parsed.max, max.spec);
                max.validate(parsed.strategy.max);
                Assert.assertEquals(parsed.minDelta, DEFAULT_MIN_DELTA_VALIDATOR.spec);
                DEFAULT_MIN_DELTA_VALIDATOR.validate(parsed.strategy.minDelta);
            }
            ParsedStrategy parsed = parseStrategy("min=" + min.spec);
            Assert.assertEquals(parsed.min, min.spec);
            min.validate(parsed.strategy.min);
            Assert.assertEquals(parsed.max, DEFAULT_MAX_VALIDATOR.spec);
            DEFAULT_MAX_VALIDATOR.validate(parsed.strategy.max);
            Assert.assertEquals(parsed.minDelta, DEFAULT_MIN_DELTA_VALIDATOR.spec);
            DEFAULT_MIN_DELTA_VALIDATOR.validate(parsed.strategy.minDelta);
        }
    }

    @Test
    public void boundParseTest()
    {
        VALIDATE.forEach(v -> v.validate(parseBound(v.spec, v.isMin)));
    }

    @Test
    public void boundParseFailureTest()
    {
        assertParseFailure("10ms <= p95(r) <= 5ms");
        assertParseFailure("10 <= p95(r)");
        assertParseFailure("p95(r) < 5");
        assertParseFailure("p95(x)");
        assertParseFailure("p95()");
        assertParseFailure("p95");
        assertParseFailure("p50(rw)+0.66");
    }
}
