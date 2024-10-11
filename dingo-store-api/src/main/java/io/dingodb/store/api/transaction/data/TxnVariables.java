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

package io.dingodb.store.api.transaction.data;

/**
 * 事务中使用的配置参数。
 */
public class TxnVariables {
    /**
     * 事务的超时时间。
     */
    public static final long TimeOut = 50000L;

    /**
     * 解决事务写入冲突时的重试间隔时间。
     */
    public static final long WaitTime = 100L;

    /**
     * 解决事务写入冲突时的固定间隔时间，重试了WaitFixNum次之后，每次重试的间隔时间就变为WaitFixTime了。
     */
    public static final long WaitFixTime = 1000L;

    /**
     * 解决事务写入冲突时，经过WaitFixNum次后就采用固定间隔时间进行尝试了。
     */
    public static final int WaitFixNum = 11;
}
