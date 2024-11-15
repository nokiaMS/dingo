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

package io.dingodb.common.type;

import io.dingodb.expr.runtime.type.AnyType;
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.BytesType;
import io.dingodb.expr.runtime.type.DateType;
import io.dingodb.expr.runtime.type.DecimalType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.TimeType;
import io.dingodb.expr.runtime.type.TimestampType;
import io.dingodb.expr.runtime.type.TypeVisitorBase;
import io.dingodb.serial.schema.BooleanListSchema;
import io.dingodb.serial.schema.BytesListSchema;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.DoubleListSchema;
import io.dingodb.serial.schema.FloatListSchema;
import io.dingodb.serial.schema.IntegerListSchema;
import io.dingodb.serial.schema.LongListSchema;
import io.dingodb.serial.schema.StringListSchema;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * 用于处理各种类型的list对应的序列化schema。
 */
public final class TypeUtils {
    /**
     * 构造函数。
     */
    private TypeUtils() {
    }

    /**
     * 返回各种元素类型list的序列化接口。
     */
    public static DingoSchema elementTypeToDingoList(@NonNull DingoType elementType) {
        return new TypeVisitorBase<DingoSchema, Void>() {

            //int list接口。
            @Override
            public DingoSchema visitIntType(@NonNull IntType type, Void obj) {
                return new IntegerListSchema(-1);
            }

            //long list接口。
            @Override
            public DingoSchema visitLongType(@NonNull LongType type, Void obj) {
                return new LongListSchema(-1);
            }

            //float list接口。
            @Override
            public DingoSchema visitFloatType(@NonNull FloatType type, Void obj) {
                return new FloatListSchema(-1);
            }

            //double list接口。
            @Override
            public DingoSchema visitDoubleType(@NonNull DoubleType type, Void obj) {
                return new DoubleListSchema(-1);
            }

            //bool list接口。
            @Override
            public DingoSchema visitBoolType(@NonNull BoolType type, Void obj) {
                return new BooleanListSchema(-1);
            }

            //decimal list接口。
            @Override
            public DingoSchema visitDecimalType(@NonNull DecimalType type, Void obj) {
                return new BytesListSchema(-1);
            }

            //string list接口。
            @Override
            public DingoSchema visitStringType(@NonNull StringType type, Void obj) {
                return new StringListSchema(-1);
            }

            //bytes list接口。
            @Override
            public DingoSchema visitBytesType(@NonNull BytesType type, Void obj) {
                return new BytesListSchema(-1);
            }

            //date list接口。
            @Override
            public DingoSchema visitDateType(@NonNull DateType type, Void obj) {
                return new LongListSchema(-1);
            }

            //time list接口。
            @Override
            public DingoSchema visitTimeType(@NonNull TimeType type, Void obj) {
                return new LongListSchema(-1);
            }

            //timestamp list接口。
            @Override
            public DingoSchema visitTimestampType(@NonNull TimestampType type, Void obj) {
                return new LongListSchema(-1);
            }

            //anyType list接口。
            @Override
            public DingoSchema visitAnyType(@NonNull AnyType type, Void obj) {
                return new BytesListSchema(-1);
            }
        }.visit(elementType.getType());
    }
}
