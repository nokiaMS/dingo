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
 * rollback命令实现类。
 */
public class RollbackTxOperation implements DdlOperation {

    /**
     * 当前连接对象。
     */
    private Connection connection;

    /**
     * 构造函数。
     * @param connection
     */
    public RollbackTxOperation(Connection connection) {
        this.connection = connection;
    }

    /**
     * 覆盖函数接口。
     */
    @Override
    public void execute() {
        try {
            //获得默认的事务服务并调用rollback接口。
            TransactionService.getDefault().rollback(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
