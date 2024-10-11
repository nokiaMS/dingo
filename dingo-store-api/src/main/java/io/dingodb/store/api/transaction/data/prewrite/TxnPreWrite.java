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

package io.dingodb.store.api.transaction.data.prewrite;

import io.dingodb.common.CommonId;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

/**
 * 事务的预写主键请求。preWritePrimaryKey。
 */
@Getter
@Setter
@Builder
@ToString
public class TxnPreWrite {
    //事务的隔离级别，默认为读已提交。
    @Builder.Default
    private IsolationLevel isolationLevel = IsolationLevel.ReadCommitted;

    //事务包含的mutation列表。
    // The data to be written to the database.
    private List<Mutation> mutations;

    //主锁（主锁中包含的内容就是primary key的值。）
    // The primary lock of the transaction is setup by client
    private byte[] primaryLock;

    //事务的开始时间。
    // Identifies the transaction being written.
    private long startTs;

    //锁超时时间，不设置则默认为1秒。
    // the lock's ttl is timestamp in milisecond.
    @Builder.Default
    private long lockTtl = 1000L;

    //事务中mutation数量。
    // the number of keys involved in the transaction
    private long txnSize;

    //告诉store是否使用1pc的标志位，true：使用1pc方式处理事务，false：使用2pc方式处理事务。
    // When the transaction involves only one region, it's possible to commit the
    // transaction directly with 1PC protocol.
    @Builder.Default
    boolean tryOnePc = false;  // NOT IMPLEMENTED


    // The max commit ts is reserved for limiting the commit ts of 1PC, which can be used to avoid inconsistency with
    // schema change. This field is unused now.
    @Builder.Default
    private long maxCommitTs = 0L;  // NOT IMPLEMENTED

    //悲观锁检测标志位，告诉store是否使用悲观锁检测。
    // for pessimistic transaction
    // check if the keys is locked by pessimistic transaction
    @Builder.Default
    List<PessimisticCheck> pessimisticChecks = Collections.emptyList();

    //
    // fo pessimistic transaction
    // for_update_ts constriants that should be checked when prewriting a pessimistic transaction.
    @Builder.Default
    List<ForUpdateTsCheck> forUpdateTsChecks = Collections.emptyList();

    //锁数据。
    // for both pessimistic and optimistic transaction
    // the extra_data executor want to store in lock
    @Builder.Default
    List<LockExtraData> lockExtraDatas = Collections.emptyList();

    CommonId txnId;
}
