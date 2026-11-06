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

package org.apache.cassandra.service.accord.execution.deadline;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

/**
 * Static configuration for deadline-ordered delivery of contended PreAccepts.
 */
public final class PreAcceptDeadlines
{
    public static final PreAcceptPenalty PENALTY;
    public static final boolean ENABLED;
    public static final long CONTENTION_WINDOW_MICROS;
    public static final long MAX_PENALTY_MICROS;
    public static final long MAX_PENALTY_NANOS;

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        ParameterizedClass penalty = config == null ? null : config.preaccept_deadline_penalty;
        PENALTY = penalty == null ? PreAcceptPenalty.NONE
                                  : ParameterizedClass.newInstance(penalty, Arrays.asList("", PreAcceptPenalty.class.getPackage().getName()), PreAcceptPenalty.class);
        ENABLED = PENALTY != PreAcceptPenalty.NONE;
        CONTENTION_WINDOW_MICROS = config == null ? 0 : config.preaccept_deadline_contention_window.to(TimeUnit.MICROSECONDS);
        MAX_PENALTY_MICROS = config == null ? 0 : config.preaccept_deadline_max_penalty.to(TimeUnit.MICROSECONDS);
        MAX_PENALTY_NANOS = TimeUnit.MICROSECONDS.toNanos(MAX_PENALTY_MICROS);
    }

    private PreAcceptDeadlines() {}
}
