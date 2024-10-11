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

package io.dingodb.common.meta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * schemaInfo结构体包含了schma的基本信息，包括租户id, schema名称，schema id及schema状态。
 *
 * @builder注释：为类生成一个包含所有字段的构造函数。
 * @data注释：
 *      可以自动为类生成getter,setter,equeals,hashcode,toString等方法。
 *      https://blog.51cto.com/u_16213447/9239694
 * @AllArgsConstructor注释：
 *      自动为类生成一个包含所有参数的构造函数。
 */
@Builder
@Data
@AllArgsConstructor
public class SchemaInfo {
    private long tenantId;      //租户id。
    private String name;        //schema名称。
    private long schemaId;      //schema id。
    private SchemaState schemaState;    //schema状态。

    /**
     * 默认构建函数。
     */
    public SchemaInfo() {

    }

    /**
     * 拷贝函数。
     * @return
     */
    public SchemaInfo copy() {
        return new SchemaInfo(tenantId, name, schemaId, schemaState);
    }
}
