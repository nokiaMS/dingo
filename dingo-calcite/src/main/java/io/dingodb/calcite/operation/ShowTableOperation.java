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
 * 'show tables' 命令实现类。
 */
public class ShowTableOperation extends QueryOperation {
    /**
     * 表所在的schema名称。
     */
    private String schemaName;

    /**
     * 当前连接。
     */
    Connection connection;

    /**
     * 郭旭添加，是否执行测试代码的标志位，true执行测试代码，false执行dingo正常代码。
     */
    private boolean forTest = false;

    private String sqlLikePattern;

    /**
     * 初始化函数。
     * @param schemaName
     * @param connection
     * @param pattern
     */
    public ShowTableOperation(String schemaName, Connection connection, String pattern) {
        this.schemaName = schemaName;
        this.connection = connection;
        this.sqlLikePattern = pattern;
    }

    /**
     * show tables命令结果迭代器，用于返回结果数据集。
     * @return
     */
    @Override
    public Iterator getIterator() {
        if(this.forTest) {
            List<Object[]> tables = new ArrayList<>();

            Object[] obj1 = new Object[] {"testTbl1", "testCol1_1", "testCol2_1"};
            Object[] obj2 = new Object[] {"testTbl2", "testCol1_2", "testCol2_2"};
            Object[] obj3 = new Object[] {"testTbl3", "testCol1_3", "testCol2_3"};
            tables.add(obj1);
            tables.add(obj2);
            tables.add(obj3);

            return tables.iterator();
        } else {
            try {
                //tables变量是一个List，是因为可能会返回多个表，一个表就是List的一个元素；List的元素类型是Obj
                List<Object[]> tables = new ArrayList<>();
                //获取结果集。
                ResultSet rs = connection.getMetaData().getTables(null, schemaName.toUpperCase(),
                    null, null);
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (StringUtils.isBlank(sqlLikePattern) || SqlLikeUtils.like(tableName, sqlLikePattern)) {
                        Object[] tuples = new Object[]{tableName.toLowerCase()};
                        tables.add(tuples);
                    }
                }
                return tables.iterator();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * SHOW TABLES命令返回结果的列名称列表。(即返回列的元信息。)
     * @return
     */
    @Override
    public List<String> columns() {
        if(this.forTest) {
            List<String> columns = new ArrayList<>();
            //设置列名称。
            columns.add("Tables_for_test_in_" + schemaName);
            columns.add("test_col_1");
            columns.add("test_col_2");
            return columns;
        } else {
            List<String> columns = new ArrayList<>();
            //设置列名称。
            columns.add("Tables_in_" + schemaName);
            return columns;
        }
    }
}

/**
 * 开启forTest开关之后的展示结果如下：
 * mysql> show tables;
 * +--------------------------+------------+------------+
 * | Tables_for_test_in_DINGO | test_col_1 | test_col_2 |
 * +--------------------------+------------+------------+
 * | testTbl1                 | testCol1_1 | testCol2_1 |
 * | testTbl2                 | testCol1_2 | testCol2_2 |
 * | testTbl3                 | testCol1_3 | testCol2_3 |
 * +--------------------------+------------+------------+
 * 3 rows in set (0.01 sec)
 *
 * mysql>
 */
