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

import io.dingodb.common.ddl.TableInfoCache;
import io.dingodb.common.log.LogUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.meta.ddl.InfoSchemaBuilder.bucketIdx;

/**
 * InfoSchema定义了一个db下所有的schema。
 */
@Slf4j
@Data
public class InfoSchema {
    /**
     * schema元信息版本号。
     */
    public long schemaMetaVersion;

    /**
     * schema与其下的所有表的关联关系。
     *      Map< schema名称,schema下的所有表 >
     */
    public Map<String, SchemaTables> schemaMap;

    public Map<Integer, List<TableInfoCache>> sortedTablesBuckets;

    /**
     * 构造函数。
     */
    public InfoSchema() {
        schemaMetaVersion = 0;
        this.schemaMap = new ConcurrentHashMap<>(); //ConcurrentHashMap是线程安全，支持高效并发的版本map。
        this.sortedTablesBuckets = new ConcurrentHashMap<>();
    }

    /**
     * 给定schema名称与表名称，返回对应的table对象。
     * @param schemaName    schema名称。
     * @param tableName     table名称。
     * @return  table对象。
     */
    public Table getTable(String schemaName, String tableName) {
        tableName = tableName.toUpperCase();
        if (schemaMap.containsKey(schemaName)) {
            Map<String, Table> tableMap = schemaMap.get(schemaName).getTables();
            if (tableMap != null) {
                return tableMap.get(tableName);
            }
        }
        return null;
    }

    /**
     * 从 schemaName 这个schema下删除一个表 tableName。
     * @param schemaName    schema名称。
     * @param tableName     table名称。
     * @return      删除成功：true；删除失败：false。
     */
    public boolean dropTable(String schemaName, String tableName) {
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();
        if (getSchemaMap().containsKey(schemaName)) {
            SchemaTables schemaTables = getSchemaMap().get(schemaName);
            return schemaTables.dropTable(tableName);
        }
        return false;
    }

    /**
     * 添加一个表到指定schema中。
     * @param schemaName    schema名称。
     * @param tableName     表名称。
     * @param table         表对象。
     * @return
     */
    public boolean putTable(String schemaName, String tableName, Table table) {
        if (getSchemaMap().containsKey(schemaName)) {
            SchemaTables schemaTables = schemaMap.get(schemaName);
            schemaTables.putTable(tableName, table);
            return true;
        }
        return false;
    }

    public Table getTable(long tableId) {
        int idx = bucketIdx(tableId);
        List<TableInfoCache> buckets = this.sortedTablesBuckets.get(idx);
        if (buckets == null) {
            return null;
        }
        TableInfoCache tableInfo
            = buckets.stream().filter(t -> t.getTableId() == tableId).findFirst().orElse(null);
        if (tableInfo == null) {
            return null;
        }
        SchemaTables schemaTables = schemaMap.get(tableInfo.getSchemaName());
        return schemaTables.getTables().get(tableInfo.getName());
    }

    public Table getIndex(long tableId, long indexId) {
        Table table = getTable(tableId);
        if (table == null || table.getIndexes() == null) {
            LogUtils.error(log, "[ddl] info schema get index: table is null or table indexes is null, tableId:{}, indexId:{}", tableId, indexId);
            return null;
        }
        return table.getIndexes().stream().filter(index -> index.getTableId().seq == indexId).findFirst().orElse(null);
    }

}
