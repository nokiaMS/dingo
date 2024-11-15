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

package io.dingodb.serial.schema;

import java.util.Locale;

/**
 * 定义了DingoSchema的类型，DingoSchema是用于序列化时的类型，每个类型都会转换为对应的schema，在序列化的时候类型相关的信息都存储在schema中了。
 */
public enum Type {
    BOOLEAN,
    BOOLEANLIST,
    SHORT,
    SHORTLIST,
    INTEGER,
    INTEGERLIST,
    FLOAT,
    FLOATLIST,
    LONG,
    LONGLIST,
    DOUBLE,
    DOUBLELIST,
    BYTES,
    BYTESLIST,
    STRING,
    STRINGLIST;

    private final String name;

    Type() {
        this.name = this.name().toLowerCase(Locale.ENGLISH);
    }

    public String getName() {
        return name;
    }
}
