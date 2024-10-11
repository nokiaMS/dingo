/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.store.api.transaction.data.checkstatus;

import io.dingodb.store.api.transaction.data.IsolationLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 事务状态检测消息：
 *      用于映射为store的TxnCheckTxnStatusRequest消息。
 *      此消息用于检测事务状态。
 */
@Getter
@Setter
@Builder
public class TxnCheckStatus {
    /**
     * 事务的隔离级别。
     */
    private IsolationLevel isolationLevel;
    // Primary key and lock ts together to locate the primary lock of a transaction.
    private byte[] primaryKey;
    // Starting timestamp oracle of the transaction being checked.
    private long lockTs;
    // The start timestamp oracle of the transaction which this request is part of.
    private long callerStartTs;
    // The client must specify the current time to dingo-store using this timestamp oracle.
    // It is used to check TTL timeouts. It may be inaccurate.
    private long currentTs;
}
