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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.impl.IntKey;
import accord.impl.IntKey.Routing;
import accord.primitives.Range;
import accord.utils.Gen;
import accord.utils.RandomSource;
import org.agrona.collections.LongArrayList;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class RTreeTest
{
    private static final Comparator<Routing> COMPARATOR = Comparator.naturalOrder();
    private static final RTree.Accessor<Routing, Range> ACCESSOR = new RTree.Accessor<>()
    {
        @Override
        public Routing start(Range range)
        {
            return (Routing) range.start();
        }

        @Override
        public Routing end(Range range)
        {
            return (Routing) range.end();
        }

        @Override
        public boolean contains(Range range, Routing routing)
        {
            return range.contains(routing);
        }

        @Override
        public boolean intersects(Range range, Routing start, Routing end)
        {
            return range.compareIntersecting(IntKey.range(start, end)) == 0;
        }

        @Override
        public boolean intersects(Range left, Range right)
        {
            return left.compareIntersecting(right) == 0;
        }
    };

    private static final Gen.IntGen SMALL_INT_GEN = rs -> rs.nextInt(0, 10);
    private static final int MIN_TOKEN = 0, MAX_TOKEN = 1 << 16;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final Gen.IntGen TOKEN_GEN = rs -> rs.nextInt(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Range> RANGE_GEN = rs -> {
        int a = TOKEN_GEN.nextInt(rs);
        int b = TOKEN_GEN.nextInt(rs);
        while (a == b)
            b = TOKEN_GEN.nextInt(rs);
        if (a > b)
        {
            int tmp = a;
            a = b;
            b = tmp;
        }
        return IntKey.range(a, b);
    };

    private enum Pattern
    {RANDOM, NO_OVERLP, SMALL_RANGES}

    // IntervalTree has (start, end) semantics, whereas Range has [start, end), so validation will fail!
    // IntervalTree is also very slow for insertion (as it rebuilds itself all over), which makes the test very slow
    // For this reason true uses a List with full scan semantics, and false uses IntervalTree and disables validating
    // but keeps the timing data
    private static final boolean VALIDATE = true;

    @Test
    public void test()
    {
        int samples = 10_000;
        qt().withExamples(10).check(rs -> {
            var map = create();
            var model = createModel();

            LongArrayList byToken = new LongArrayList(samples, -1);
            LongArrayList modelByToken = new LongArrayList(samples, -1);
            LongArrayList byRange = new LongArrayList(samples, -1);
            LongArrayList modelByRange = new LongArrayList(samples, -1);
            Gen<Range> rangeGen;
//            Pattern pattern = rs.pick(Pattern.values());
            Pattern pattern = Pattern.SMALL_RANGES;
            switch (pattern)
            {
                case RANDOM:
                    rangeGen = RANGE_GEN;
                    break;
                case SMALL_RANGES:
                    rangeGen = random -> {
                        int a = TOKEN_GEN.nextInt(random);
                        int rangeSize = rs.nextInt(100, (int) (TOKEN_RANGE_SIZE * .01));
                        int b = a + rangeSize;
                        if (b > MAX_TOKEN)
                        {
                            b = a;
                            a = b - rangeSize;
                        }
                        return IntKey.range(a, b);
                    };
                    break;
                case NO_OVERLP:
                    int delta = TOKEN_RANGE_SIZE / samples;
                    rangeGen = new Gen<>()
                    {
                        int idx = 0;

                        @Override
                        public Range next(RandomSource ignore)
                        {
                            int a = delta * idx++;
                            int b = a + delta;
                            return IntKey.range(a, b);
                        }
                    };
                    break;
                default:
                    throw new AssertionError();
            }
            for (int i = 0; i < samples; i++)
            {
                var range = rangeGen.next(rs);
                var value = SMALL_INT_GEN.nextInt(rs);
                map.add(range, value);
                model.put(range, value);
            }
            Assertions.assertThat(map).hasSize(samples);
            for (int i = 0; i < samples; i++)
            {
                if (rs.nextBoolean())
                {
                    // key lookup
                    var lookup = IntKey.routing(TOKEN_GEN.nextInt(rs));
                    var actual = timed(byToken, () -> map.findToken(lookup));
                    var expected = timed(modelByToken, () -> model.intersectsToken(lookup));
                    if (VALIDATE)
                        Assertions.assertThat(sort(actual))
                                  .describedAs("Write=%d; token=%s, actual=%s", i, lookup, model.actual())
                                  .isEqualTo(sort(expected));
                }
                else
                {
                    // range lookup
                    var lookup = RANGE_GEN.next(rs);
                    var actual = timed(byRange, () -> map.find(lookup));
                    var expected = timed(modelByRange, () -> model.intersects(lookup));
                    if (VALIDATE)
                        Assertions.assertThat(sort(actual))
                                  .describedAs("Write=%d; range=%s, actual=%s", i, lookup, model.actual())
                                  .isEqualTo(sort(expected));
                }
            }
            System.out.println("=======");
            System.out.println(map.displayTree());
            System.out.println("Pattern: " + pattern);
            long modelCost = ObjectSizes.measureDeep(model.actual());
            System.out.println("Model Memory Cost: " + modelCost);
            long actualCost = ObjectSizes.measureDeep(map);
            System.out.println("Memory Cost: " + actualCost + "; " + String.format("%.2f", ((actualCost - modelCost) / (double) actualCost * 100.0)) + "% model size");
            System.out.println("=======");
            System.out.println("By Token: " + stats(byToken));
            System.out.println("Model By Token: " + stats(modelByToken));
            System.out.println("By Range: " + stats(byRange));
            System.out.println("Model By Range: " + stats(modelByRange));
        });
    }

    private static String stats(LongArrayList list)
    {
        long[] array = list.toLongArray();
        Arrays.sort(array);
        StringBuilder sb = new StringBuilder();
        sb.append("Min: ").append(TimeUnit.NANOSECONDS.toMicros(array[0])).append("micro");
        sb.append(", Median: ").append(TimeUnit.NANOSECONDS.toMicros(array[array.length / 2])).append("micro");
        sb.append(", Max: ").append(TimeUnit.NANOSECONDS.toMicros(array[array.length - 1])).append("micro");
        return sb.toString();
    }

    private static <T> T timed(LongArrayList target, Supplier<T> fn)
    {
        long nowNs = System.nanoTime();
        try
        {
            return fn.get();
        }
        finally
        {
            target.add(System.nanoTime() - nowNs);
        }
    }

    private static <T extends Comparable<T>> List<T> sort(List<T> list)
    {
        list.sort(Comparator.naturalOrder());
        return list;
    }

    private static RTree<Routing, Range, Integer> create()
    {
        return new RTree<>(COMPARATOR, ACCESSOR);
    }

    private interface Model
    {
        Object actual();
        void put(Range range, int value);

        List<Integer> intersectsToken(Routing key);

        List<Integer> intersects(Range range);
    }

    private static Model createModel()
    {
        return VALIDATE ? new ListModel() : new IntervalTreeModel();
    }

    private static class ListModel implements Model
    {
        List<Pair<Range, Integer>> actual = new ArrayList<>();

        @Override
        public List<Pair<Range, Integer>> actual()
        {
            return actual;
        }

        @Override
        public void put(Range range, int value)
        {
            actual.add(Pair.create(range, value));
        }

        @Override
        public List<Integer> intersectsToken(Routing key)
        {
            return actual.stream()
                         .filter(p -> p.left.contains(key))
                         .map(p -> p.right)
                         .collect(Collectors.toList());
        }

        @Override
        public List<Integer> intersects(Range range)
        {
            return actual.stream()
                         .filter(p -> p.left.compareIntersecting(range) == 0)
                         .map(p -> p.right)
                         .collect(Collectors.toList());
        }
    }

    private static class IntervalTreeModel implements Model
    {
        IntervalTree<Routing, Integer, Interval<Routing, Integer>> actual = IntervalTree.emptyTree();

        @Override
        public IntervalTree<Routing, Integer, Interval<Routing, Integer>> actual()
        {
            return actual;
        }

        @Override
        public void put(Range range, int value)
        {
            actual = actual.unbuild().add(new Interval<>((Routing) range.start(), (Routing) range.end(), value)).build();
        }

        @Override
        public List<Integer> intersectsToken(Routing key)
        {
            return actual.search(key);
        }

        @Override
        public List<Integer> intersects(Range range)
        {
            return actual.search(new Interval<>((Routing) range.start(), (Routing) range.end(), null));
        }
    }
}