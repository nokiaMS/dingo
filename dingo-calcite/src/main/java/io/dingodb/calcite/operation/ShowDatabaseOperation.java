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

import io.dingodb.common.util.SqlLikeUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * show databases 操作的响应对象。
 */
public class ShowDatabaseOperation extends QueryOperation {

    /**
     * 连接对象。
     */
    Connection connection;

    private final String sqlLikePattern;

    /**
     * 构造函数。
     * @param connection
     * @param sqlLikePattern
     */
    public ShowDatabaseOperation(Connection connection, String sqlLikePattern) {
        this.connection = connection;
        this.sqlLikePattern = sqlLikePattern;
    }

    /**
     * 结果集迭代器。
     * @return
     */
    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> schemas = new ArrayList<>();
        try {
            //从原信息中获取schema列表。
            ResultSet rs = connection.getMetaData().getSchemas();
            while (rs.next()) {
                //只获得结果集中的TABLE_SCHEM列内容。
                String schemaName = rs.getString("TABLE_SCHEM");
                //不显示root schema。
                if (schemaName.equalsIgnoreCase("ROOT")) {
                    continue;
                }

                //如果没有配置like模式或者匹配到了like模式，那么把schema加入到结果集中。
                if (StringUtils.isBlank(sqlLikePattern) || SqlLikeUtils.like(schemaName, sqlLikePattern)) {
                    schemas.add(new Object[] {schemaName.toLowerCase()});
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //返回结果集迭代器。
        return schemas.iterator();
    }

    /**
     * 结果元信息。
     * @return
     */
    @Override
    public List<String> columns() {
        List<String> columns =  new ArrayList<>();
        //结果只有一列。
        columns.add("TABLE_SCHEM");
        return columns;
    }
}
