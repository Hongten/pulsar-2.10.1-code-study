/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.coordinator;

import com.google.common.annotations.Beta;
import java.util.concurrent.CompletableFuture;

/**
 * // todo 服务端用于追踪超时的事务
 * Represent the tracker for the timeout of the transaction.
 */
@Beta
public interface TransactionTimeoutTracker extends AutoCloseable {

    /**
     * Add a txnID to the tracker.
     *
     * @param sequenceId
     *            the sequenceId
     * @param timeout
     *            the absolute timestamp for transaction timeout
     *
     * @return true if the transaction was added to the tracker or false if had timed out
     */
    CompletableFuture<Boolean> addTransaction(long sequenceId, long timeout);

    /**
     * When replay the log, add the txnMeta to timer task queue.
     *
     * @param sequenceId
     *            the sequenceId
     * @param timeout
     *            the absolute timestamp for transaction timeout
     */
    void replayAddTransaction(long sequenceId, long timeout);

    /**
     * When replay the log finished, we need to start the tracker.
     */
    void start();

    /**
     * Close the transaction timeout tracker and release all resources.
     */
    void close();
}
