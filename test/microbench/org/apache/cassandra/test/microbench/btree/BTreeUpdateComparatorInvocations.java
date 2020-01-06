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

package org.apache.cassandra.test.microbench.btree;

public class BTreeUpdateComparatorInvocations
{
    private static int comparisons = 0;

    public static void main(String[] args)
    {
        boolean forever = false;

        int[] insertSizes = new int[] { 1, 4, 16, 64, 256, 1024, 16384 };
        int[] dataSizes   = new int[] { 1, 4, 16, 64, 256, 1024, 16384 };
        BTreeUpdateBench.Distribution[] distributions = BTreeUpdateBench.Distribution.values();
        boolean[] keepOlds = new boolean[] { false, true };
        float[] overlaps = new float[] { 0.0f, 0.5f, 1.0f };
        BTreeBench.UpdateF[] updateFs = new BTreeBench.UpdateF[] { BTreeBench.UpdateF.SIMPLE };

//        int[] insertSizes = new int[] { 16384 };
//        int[] dataSizes   = new int[] { 1024 };
//        BTreeUpdateBench.Distribution[] distributions = new BTreeUpdateBench.Distribution[] { BTreeUpdateBench.Distribution.RANDOM };
//        boolean[] keepOlds = new boolean[] { true };
//        float[] overlaps = new float[] { 0f };
//        BTreeBench.UpdateF[] updateFs = new BTreeBench.UpdateF[] { BTreeBench.UpdateF.SIMPLE };

        BTreeUpdateBench bench = new BTreeUpdateBench();
        bench.comparator = () -> (a, b) -> {
            ++comparisons;
            return a.compareTo(b);
        };

        boolean uniquePerTrial = false;
        if (forever && (insertSizes.length != 1 || dataSizes.length != 1 || distributions.length != 1 || keepOlds.length != 1 || overlaps.length != 1 || updateFs.length != 1))
            throw new IllegalStateException();

        for (int insertSize : insertSizes)
        {
            for (int dataSize : dataSizes)
            {
                for (BTreeUpdateBench.Distribution distribution : distributions)
                {
                    for (boolean keepOld : keepOlds)
                    {
                        for (float overlap : overlaps)
                        {
                            for (BTreeBench.UpdateF updateF : updateFs)
                            {
                                bench.insertSize = insertSize;
                                bench.uniquePerTrial = uniquePerTrial;
                                bench.dataSize = dataSize;
                                bench.distribution = distribution;
                                bench.keepOld = keepOld;
                                bench.overlap = overlap;
                                bench.updateF = updateF;
                                BTreeUpdateBench.ThreadState invocationState = new BTreeUpdateBench.ThreadState();
                                bench.setup();
                                BTreeUpdateBench.BuildSizeState buildSizeState = new BTreeBench.BuildSizeState();
                                BTreeUpdateBench.InsertSizeState insertSizeState = new BTreeUpdateBench.InsertSizeState();
                                invocationState.doTrialSetup(bench, buildSizeState, insertSizeState);
                                do
                                {
                                    for (int i = 0 ; i < 1024 ; ++i)
                                    {
                                        invocationState.doInvocationSetup(buildSizeState, insertSizeState);
                                        bench.benchUpdate(invocationState);
                                    }
                                } while (forever);
                                System.out.printf("%d %s %d %b %.1f %b %s %.2f\n", dataSize, distribution, insertSize, keepOld, overlap, uniquePerTrial, updateF, comparisons / (float) 1024);
                                comparisons = 0;
                            }
                        }
                    }
                }
            }
        }
    }
}
