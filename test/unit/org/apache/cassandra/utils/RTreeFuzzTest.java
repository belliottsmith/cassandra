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

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class RTreeFuzzTest
{
    private enum Action { Create, Read, Update, Delete }
    // uniform means we don't split often, but leaving here as I have a demo and this is a good example why bias can be useful
    private static final Gen<Action> ACTION_GEN = Gens.enums().all(Action.class);

    @Test
    public void mapLike()
    {
        qt().check(rs -> {
            TreeMap<Integer, Integer> model = new TreeMap<>();
            var allActions = rs.randomWeightedPicker(Action.values());
            var tree = intTree();
            for (int i = 0; i < 1000; i++)
            {
                // should cleanup dead code, but leaving here for a demo in a few weeks...
//                Gen<Action> actionGen = model.isEmpty() ? ignore -> Action.Create : ACTION_GEN;
//                switch (actionGen.next(rs))
                Supplier<Action> actionGen = model.isEmpty() ? () -> Action.Create : allActions;
                switch (actionGen.get())
                {
                    case Create:
                    {
                        int key = rs.nextInt();
                        while (model.containsKey(key))
                            key = rs.nextInt();
                        int value = rs.nextInt();
                        model.put(key, value);
                        tree.add(key, value);
                        Assertions.assertThat(tree.size())
                                  .isEqualTo(model.size());
                    }
                    break;
                    case Read:
                    {
                        int key = rs.pick(model.keySet());
                        int expected = model.get(key);
                        Assertions.assertThat(tree.get(key))
                                  .isEqualTo(Arrays.asList(expected));
                    }
                    break;
                    case Update:
                    {
                        int key = rs.pick(model.keySet());
                        int before = model.get(key);
                        int after = rs.nextInt();
                        while (before == after)
                            after = rs.nextInt();
                        model.put(key, after);
                        int finalAfter = after;
                        Assertions.assertThat(tree.get(key, e -> e.setValue(finalAfter)))
                                  .isTrue();
                        Assertions.assertThat(tree.size())
                                  .isEqualTo(model.size());
                        // this validation is a duplication of the Read action, it is mostly here just to make sure we
                        // will read these updated keys, and if we do find an issue fail fast
                        Assertions.assertThat(tree.get(key))
                                  .isEqualTo(Arrays.asList(after));
                    }
                    break;
                    case Delete:
                    {
                        int key = rs.pick(model.keySet());
                        model.remove(key);
                        Assertions.assertThat(tree.remove(key))
                                  .isEqualTo(1);
                        Assertions.assertThat(tree.size())
                                  .isEqualTo(model.size());
                    }
                    break;
                    default:
                        throw new AssertionError("Unexpected action");
                }
            }
        });
    }

    private static RTree<Integer, Integer, Integer> intTree()
    {
        return RTree.create(new RTree.Accessor<>()
        {
            @Override
            public Integer start(Integer integer)
            {
                return integer.intValue() - 1;
            }

            @Override
            public Integer end(Integer integer)
            {
                return integer;
            }

            @Override
            public boolean contains(Integer integer, Integer integer2)
            {
                return integer.equals(integer2);
            }

            @Override
            public boolean intersects(Integer integer, Integer start, Integer end)
            {
                return start < integer && integer <= end;
            }
        });
    }
}
