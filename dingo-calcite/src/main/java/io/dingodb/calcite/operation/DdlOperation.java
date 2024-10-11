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

package io.dingodb.calcite.operation;

import io.dingodb.common.profile.Profile;

/**
 * ddl操作接口。
 * ddl操作通常不需要返回结果集，如果需要返回结果集那么可以继承QueryOperation.
 */
public interface DdlOperation extends Operation {
    /**
     * ddl操作需要实现doExecute接口。
     * 如果没有实现的话那么采用默认实现，因此，继承类只需要实现execute()接口即可。
     * @param profile
     */
    default void doExecute(Profile profile) {
        execute();
        profile.end();
    }

    /**
     * sql命令的执行函数，继承类必须实现此接口。
     */
    void execute();
}
