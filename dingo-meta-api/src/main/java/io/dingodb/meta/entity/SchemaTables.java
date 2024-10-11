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

package io.dingodb.meta.entity;


import io.dingodb.common.meta.SchemaInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * SchemaTables类定义了一个schema下的所有table信息的结构体。
 *      其实就是定义了schema与schema下所有表的关联关系。
 */
@Data
public class SchemaTables {
    /**
     * 当前schema基本信息。
     */
    private SchemaInfo schemaInfo;

    /**
     * schema下对应的表：Map<表名，表对象>
     */
    private Map<String, Table> tables;

    /**
     * 构造函数。
     * @param schemaInfo
     */
    public SchemaTables(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        this.tables = new ConcurrentHashMap<>();    //线程安全，高并发的map。
    }

    /**
     * 构造函数
     * @param schemaInfo
     * @param tables
     */
    public SchemaTables(SchemaInfo schemaInfo, Map<String, Table> tables) {
        this.schemaInfo = schemaInfo;
        this.tables = tables;
    }

    /**
     * 默认构造函数。
     */
    public SchemaTables() {
        this.tables = new ConcurrentHashMap<>();
    }

    /**
     * 从schema中删除一个表。
     * @param tableName 表名。
     * @return
     */
    public boolean dropTable(String tableName) {
        this.tables.remove(tableName);
        return true;
    }

    /**
     * 添加一个表到schema中。
     * @param tableName
     * @param table
     */
    public void putTable(String tableName, Table table) {
        this.tables.put(tableName, table);
    }

    /**
     * 拷贝SchemaTables对象的拷贝函数。
     * @return  返回一个新的SchemaTables对象，包含旧数据。
     */
    public SchemaTables copy() {
        SchemaTables schemaTables = new SchemaTables();
        schemaTables.setSchemaInfo(schemaInfo.copy());
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            List<Column> copyColList = entry.getValue().columns
             .stream()
             .map(Column::copy)
             .collect(Collectors.toList());
            schemaTables.tables.put(entry.getKey(), entry.getValue().copyWithColumns(copyColList));
        }
        return schemaTables;
    }

}
