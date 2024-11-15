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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.serial.io.RecordDecoder;
import io.dingodb.serial.io.RecordEncoder;
import io.dingodb.serial.schema.DingoSchema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.List;

public class DingoCodec implements Codec {
    TupleMapping mapping;
    private final RecordEncoder re;
    private final RecordDecoder rd;

    public DingoCodec(List<DingoSchema> schemas) {
        this(schemas, null, false);
    }

    public DingoCodec(List<DingoSchema> schemas, TupleMapping mapping) {
        this(schemas, mapping, false);
    }

    /**
     * 编码器。
     * @param schemas   待编码的schema列表。
     * @param mapping   编码列的位置信息映射。
     * @param isKey     是否是对key进行编码。
     */
    public DingoCodec(List<DingoSchema> schemas, TupleMapping mapping, boolean isKey) {
        //构造record encoder。
        this.re = new RecordEncoder(schemas, (short) 0,
            CodeTag.UNFINISHFALG, CodeTag.FINISHEDFALG, CodeTag.DELETEFLAG, null, isKey);
        //构造record decoder。
        this.rd = new RecordDecoder(schemas, (short) 0,
            CodeTag.UNFINISHFALG, CodeTag.FINISHEDFALG, CodeTag.DELETEFLAG, null, isKey);
        //存储待编码列的位置信息。
        this.mapping = mapping;
    }

    @Override
    public byte[] encode(Object[] tuple) throws IOException, ClassCastException {
        return re.encode(tuple);
    }

    @Override
    public byte[] encode(Object[] tuple, @NonNull TupleMapping mapping)
        throws IOException, ClassCastException {
        Object[] newTuple = new Object[mapping.size()];
        int i = 0;
        for (int index : mapping.getMappings()) {
            newTuple[index] = tuple[i++];
        }
        return re.encode(newTuple);
    }

    @Override
    public byte[] encode(byte[] origin, Object[] tuple, int[] schemaIndex)
        throws IOException, ClassCastException {
        return re.encode(origin, schemaIndex, tuple);
    }

    /**
     * 对key部分编码。
     * @param tuple     待编码的key部分。
     * @return
     * @throws IOException
     * @throws ClassCastException
     */
    @Override
    public byte[] encodeKey(Object[] tuple) throws IOException, ClassCastException {
        return re.encodeKey(tuple);
    }

    @Override
    public byte[] encodeKey(Object[] tuple, @NonNull TupleMapping mapping) throws IOException, ClassCastException {
        Object[] newTuple = new Object[mapping.size()];
        int i = 0;
        for (int index : mapping.getMappings()) {
            newTuple[index] = tuple[i++];
        }
        return re.encodeKey(newTuple);
    }

    @Override
    public byte[] encodeKey(byte[] origin, Object[] tuple, int[] schemaIndex) throws IOException, ClassCastException {
        return re.encodeKey(origin, schemaIndex, tuple);
    }

    @Override
    public byte[] encodeKeyForRangeScan(Object[] tuple, int columnCount) throws IOException, ClassCastException {
        return re.encodeKeyWithoutLength(tuple, columnCount);
    }

    @Override
    public Object[] decode(byte[] bytes) throws IOException {
        return rd.decode(bytes);
    }

    @Override
    public Object[] decode(Object[] result, byte[] bytes, @NonNull TupleMapping mapping) throws IOException {
        Object[] tuple = decode(bytes);
        mapping.map(result, tuple);
        return result;
    }

    @Override
    public Object[] decode(byte[] bytes, int[] schemaIndex) throws IOException {
        return rd.decode(bytes, schemaIndex);
    }

    @Override
    public Object[] decodeKey(byte[] bytes) throws IOException {
        return rd.decodeKey(bytes);
    }

    @Override
    public Object[] decodeKey(Object[] result, byte[] bytes, @NonNull TupleMapping mapping) throws IOException {
        Object[] tuple = decodeKey(bytes);
        mapping.map(result, tuple);
        return result;
    }

    @Override
    public Object[] decodeKey(byte[] bytes, int[] schemaIndex) throws IOException {
        return rd.decodeKey(bytes, schemaIndex);
    }
}
