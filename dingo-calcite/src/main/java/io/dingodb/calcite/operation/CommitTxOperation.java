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

import io.dingodb.transaction.api.TransactionService;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * commit 命令响应函数。
 */
public class CommitTxOperation implements DdlOperation {

    /**
     * 当前连接对象。
     */
    private Connection connection;

    /**
     * 构造函数。
     * @param connection    当前连接对象。
     */
    public CommitTxOperation(Connection connection) {
        this.connection = connection;
    }

    /**
     * 接口重载，sql命令的执行函数。
     */
    @Override
    public void execute() {
        try {
            //获得事务服务实例并执行“提交”操作。
            TransactionService.getDefault().commit(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
