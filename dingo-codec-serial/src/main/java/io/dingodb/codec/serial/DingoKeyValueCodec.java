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

package io.dingodb.codec.serial;

import io.dingodb.codec.Codec;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.DingoConverter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * 序列化。
 */
@Slf4j
public class DingoKeyValueCodec implements KeyValueCodec {

    /**
     * 一个tuple的元信息，例如：tuple[int, int, char]
     */
    private final DingoType schema;

    /**
     * 主键的元信息，如果1,2列是主键，那么主键的元信息就是tuple[int, int]
     */
    private final DingoType keySchema;

    /**
     * 主键列在tuple中的位置映射，
     */
    TupleMapping keyMapping;

    /**
     * 一个tuple的非主键列的位置映射。
     */
    TupleMapping valueMapping;

    /**
     * key编码器。
     */
    Codec keyCodec;

    /**
     * 非主键编码器。
     */
    Codec valueCodec;

    /**
     * 序列化（给定了参数之后，就构建出了key与value的序列化器，之后给定具体的值的时候，使用这两个序列化器就能够进行一行的key与value的序列化了。）
     * @param schema    表的tuple结构，例如：tuple[int, int, char]
     * @param keyMapping    表的主键列的位置列表。
     */
    public DingoKeyValueCodec(@NonNull DingoType schema, TupleMapping keyMapping) {
        //一个表的元信息
        this.schema = schema;
        //key元信息（此处的key即编码后k-v结构的key部分）
        this.keySchema = schema.select(keyMapping);
        //key的位置映射。
        this.keyMapping = keyMapping;
        //非主键列（即待编入value部分的列）的位置映射。
        this.valueMapping = keyMapping.inverse(schema.fieldCount());

        //构造key的编码器。
        keyCodec = new DingoCodec(schema.select(keyMapping).toDingoSchemas(), keyMapping, true);
        //构造value的编码器。
        valueCodec = new DingoCodec(schema.select(valueMapping).toDingoSchemas(), valueMapping, false);
    }

    @Override
    @SneakyThrows
    public Object[] decode(@NonNull KeyValue keyValue) {
        Object[] record = new Object[keyMapping.size() + valueMapping.size()];
        Object[] key = keyCodec.decodeKey(keyValue.getKey());
        Object[] value = valueCodec.decode(keyValue.getValue());
        for (int i = 0; i < key.length; i++) {
            record[keyMapping.get(i)] = key[i];
        }
        for (int i = 0; i < value.length; i++) {
            record[valueMapping.get(i)] = value[i];
        }
        return (Object[]) schema.convertFrom(record, DingoConverter.INSTANCE);
    }

    @Override
    @SneakyThrows
    public Object[] decodeKey(byte @NonNull [] key) {
        return keyCodec.decodeKey(key);
    }

    /**
     * 对value进行编码。
     * @param tuple tuple
     * @return
     */
    @Override
    @SneakyThrows
    public KeyValue encode(Object @NonNull [] tuple) {
        //把值的tuple按照行原信息进行转换，并存储转换后的结果到converted中。
        Object[] converted = (Object[]) schema.convertTo(tuple, DingoConverter.INSTANCE);
        //存储转换后的key值。
        Object[] key = new Object[keyMapping.size()];
        //存储转换后的value值。
        Object[] value = new Object[valueMapping.size()];

        //根据位置信息获得转换后的key值。
        for (int i = 0; i < keyMapping.size(); i++) {
            key[i] = converted[keyMapping.get(i)];
        }

        //根据位置信息获得转换后的value值。
        for (int i = 0; i < valueMapping.size(); i++) {
            value[i] = converted[valueMapping.get(i)];
        }

        //对key进行编码。
        byte[] keyByte = keyCodec.encodeKey(key);
        //对value进行编码。
        byte[] valueByte = valueCodec.encode(value);

        //返回编码后的KeyValue对象。
        return new KeyValue(keyByte, valueByte);
    }

    /**
     * 对key进行编码。
     * @param tuple key tuple
     * @return
     */
    @Override
    @SneakyThrows
    public byte[] encodeKey(Object[] tuple) {
        //把key的值tuple按照key的元信息转换。
        Object[] key = (Object[]) keySchema.convertTo(keyMapping.revMap(tuple), DingoConverter.INSTANCE);
        //对key进行编码。
        return keyCodec.encodeKey(key);
    }

    @Override
    @SneakyThrows
    public Object[] mapKeyAndDecodeValue(Object @NonNull [] keys, byte[] bytes) {
        Object[] value = valueCodec.decode(bytes);
        Object[] record = new Object[keyMapping.size() + valueMapping.size()];
        for (int i = 0; i < keys.length; i++) {
            record[keyMapping.get(i)] = keys[i];
        }
        for (int i = 0; i < value.length; i++) {
            record[valueMapping.get(i)] = value[i];
        }
        return (Object[]) schema.convertFrom(record, DingoConverter.INSTANCE);
    }

    @Override
    @SneakyThrows
    public byte[] encodeKeyPrefix(Object[] tuple, int count) {
        if (count == 0) {
            return keyCodec.encodeKeyForRangeScan(tuple, count);
        }
        return keyCodec.encodeKeyForRangeScan(keyMapping.revMap(tuple), count);
    }

    @Override
    @SneakyThrows
    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        return new Object[0];
    }

}
