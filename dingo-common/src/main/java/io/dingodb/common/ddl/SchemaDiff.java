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

package io.dingodb.common.ddl;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * 定义了schema对象的变动。
 * @Data:自动为各种属性生成相应函数。
 */
@ToString
@Data
public class SchemaDiff {
    /**
     * 当前schema的版本号。
     */
    long version;

    /**
     * ddl操作类型。
     */
    ActionType type;

    /**
     * 当前操作的shema id。
     */
    long schemaId;

    /**
     * 当前操作的表id。
     */
    long tableId;

    long oldTableId;
    long oldSchemaId;

    /**
     * 当前操作的表名称。
     */
    String tableName;

    boolean regenerateSchemaMap;

    AffectedOption[] affectedOpts;

    /**
     * 构造函数。
     * @param version
     * @param type
     * @param schemaId
     * @param tableId
     * @param oldTableId
     * @param oldSchemaId
     * @param regenerateSchemaMap
     * @param affectedOpts
     */
    @Builder
    public SchemaDiff(long version,
                      ActionType type,
                      long schemaId,
                      long tableId,
                      long oldTableId,
                      long oldSchemaId,
                      boolean regenerateSchemaMap,
                      AffectedOption[] affectedOpts) {
        this.version = version;
        this.type = type;
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.oldTableId = oldTableId;
        this.oldSchemaId = oldSchemaId;
        this.regenerateSchemaMap = regenerateSchemaMap;
        this.affectedOpts = affectedOpts;
    }

    /**
     * 默认构造函数。
     */
    public SchemaDiff() {

    }

}
