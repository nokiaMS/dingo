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

/**
 * ddl操作类型。
 */
public enum ActionType {
    ActionNone(0),              //none.
    ActionCreateSchema(1),      //create schema.
    ActionDropSchema(2),        //drop schema.
    ActionCreateTable(3),       //create table.
    ActionDropTable(4),         //drop table.
    ActionAddColumn(5),         //add column.
    ActionDropColumn(6),        //drop column.
    ActionAddIndex(7),          //add index.
    ActionDropIndex(8),         //drop index.
    ActionTruncateTable(11),    //truncate table.
    ActionModifyColumn(12),     //modify column.
    ActionAddPrimaryKey(32),    //add primary key.
    ActionCreateTables(60),     //create tables.
    ;

    /**
     * 存储枚举数值。
     */
    private final int code;

    /**
     * 构造函数。
     * @param code
     */
    ActionType(int code) {
        this.code = code;
    }

    /**
     * 获得枚举值。
     * @return
     */
    public long getCode() {
        return code;
    }

}
