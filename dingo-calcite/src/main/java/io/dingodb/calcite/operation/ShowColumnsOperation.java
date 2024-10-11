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

import io.dingodb.calcite.grammar.dql.SqlShowColumns;
import io.dingodb.common.util.SqlLikeUtils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * show columns from <tbl>  实现函数。
 *
 * 用例一：
 * mysql> show columns from gx3;
 * +-------+---------+------+------+---------+
 * | Field | Type    | Null | Key  | Default |
 * +-------+---------+------+------+---------+
 * | A     | INTEGER | YES  |      | NULL    |
 * +-------+---------+------+------+---------+
 * 1 row in set (0.01 sec)
 *
 * mysql>
 *
 * 用例二：
 * mysql> create table gg1(a int, b int, primary key(a));
 * Query OK, 0 rows affected (3.75 sec)
 *
 * mysql> show columns from gg1;
 * +-------+---------+------+------+---------+
 * | Field | Type    | Null | Key  | Default |
 * +-------+---------+------+------+---------+
 * | A     | INTEGER | NO   | PRI  | NULL    |
 * | B     | INTEGER | YES  |      | NULL    |
 * +-------+---------+------+------+---------+
 * 2 rows in set (0.03 sec)
 *
 * mysql>
 */
public class ShowColumnsOperation extends QueryOperation {

    @Setter
    public SqlNode sqlNode;

    /**
     * 表所在的schema。
     */
    private final String schemaName;

    /**
     * 表名。
     */
    private final String tableName;

    private final String sqlLikePattern;

    /**
     * 构造函数
     * @param sqlNode
     */
    public ShowColumnsOperation(SqlNode sqlNode) {
        //把sqlNode转换为对应类型，然后获取并填充相应字段。
        SqlShowColumns showColumns = (SqlShowColumns) sqlNode;
        this.schemaName = showColumns.schemaName;
        this.tableName = showColumns.tableName;
        this.sqlLikePattern = showColumns.sqlLikePattern;
    }

    /**
     * 结果集迭代器。
     * @return
     */
    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> tuples = new ArrayList<>();
        List<List<String>> columnList = getColumnFields();
        for (List<String> values : columnList) {
            Object[] tuple = values.toArray();
            tuples.add(tuple);
        }
        return tuples.iterator();
    }

    /**
     * 结果集元信息。
     * @return
     */
    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Field");   //列名称。
        columns.add("Type");    //列类型。
        columns.add("Null");    //是否可以为空。
        columns.add("Key");     //
        columns.add("Default"); //列的默认值。
        return columns;
    }

    /**
     * 获得表列的元信息。
     * @return
     */
    private List<List<String>> getColumnFields() {
        //根据schema及表名称获得table对象。
        InfoSchema is = DdlService.root().getIsLatest();    //guoxu: DdlService的实现机制需要再进一步分析。
        Table table = is.getTable(schemaName, tableName);
        if (table == null) {
            throw new RuntimeException("Table " + tableName + " doesn't exist");
        }

        //获得table的列信息。
        List<Column> columns = table.getColumns();
        List<List<String>> columnList = new ArrayList<>();
        boolean haveLike = !StringUtils.isBlank(sqlLikePattern);
        //处理每个列。
        for (Column column : columns) {
            if (column.getState() != 1) {
                continue;
            }
            List<String> columnValues = new ArrayList<>();

            //获得列名称。
            String columnName = column.getName();
            if (haveLike && !SqlLikeUtils.like(columnName, sqlLikePattern)) {
                continue;
            }

            columnValues.add(columnName);

            //获得列类型。
            String type = column.getSqlTypeName();
            if (type.equals("VARCHAR")) {
                if (column.getPrecision() > 0) {
                    type = type + "(" + column.getPrecision() + ")";
                }
            }
            columnValues.add(type);

            //获得列是否可为空标志位。
            columnValues.add(column.isNullable() ? "YES" : "NO");

            //判断列是否为主键。
            columnValues.add(column.isPrimary() ? "PRI" : "");

            //获得列的默认值。
            columnValues.add(column.defaultValueExpr != null ? column.defaultValueExpr : "NULL");

            columnList.add(columnValues);
        }
        return columnList;
    }
}
