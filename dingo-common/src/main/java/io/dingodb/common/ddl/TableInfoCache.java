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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 表信息缓存对象。
 */
@EqualsAndHashCode
@Data
@AllArgsConstructor
public class TableInfoCache {
    /**
     * table id。
     */
    private long tableId;

    /**
     * 表名称。
     */
    private String name;

    /**
     * schema id。
     */
    private long schemaId;

    /**
     * schema名称。
     */
    private String schemaName;

    /**
     * 深拷贝操作，创建一个新的TableInfoCache，赋值对应字段后返回新对象。
     * @return
     */
    public TableInfoCache deepCopy() {
        return new TableInfoCache(tableId, name, schemaId, schemaName);
    }
}
