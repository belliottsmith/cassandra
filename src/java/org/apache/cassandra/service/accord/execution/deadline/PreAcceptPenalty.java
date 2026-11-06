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

import accord.local.Node;
import accord.primitives.TxnId;

/**
 * Supplies the penalty to apply to a PreAccept we have judged to be contended: the PreAccept will not be processed
 * before {@code txnId.hlc() + penalty} (by the local HLC clock). Contended PreAccepts are released strictly in txnId
 * order, so a PreAccept may also be held behind any with a lower txnId and later deadline.
 *
 * Invoked while holding the executor lock, so must be cheap and non-blocking.
 *
 * See {@link LinkLatencyPreAcceptPenalty} for the standard implementation.
 */
public interface PreAcceptPenalty
{
    PreAcceptPenalty NONE = (local, coordinator, txnId) -> 0;

    /**
     * @return the penalty in micros to add to {@code txnId.hlc()}; zero or negative for none
     */
    long penaltyMicros(Node.Id local, Node.Id coordinator, TxnId txnId);
}
