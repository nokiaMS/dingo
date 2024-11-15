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

package io.dingodb.common.store;

public class KeyValue extends Row {

    /**
     * KeyValue构造函数.
     * @param primaryKey    编码后的key。
     * @param raw           编码后的value。
     */
    public KeyValue(byte[] primaryKey, byte[] raw) {
        super(primaryKey, new int[0], new int[0], new byte[][] {raw});
    }

    /**
     * 设置key.
     * @param key
     */
    public void setKey(byte[] key) {
        primaryKey = key;
    }

    /**
     * 设置值。
     * @param value
     */
    public void setValue(byte[] value) {
        //在实际存储中，一维数组与二维数组的存储形式是一致的，因此此处以columns[0]的方式存储了二维数字的字节流。
        columns[0] = value;
    }

    /**
     * 获得key。
     * @return
     */
    public byte[] getKey() {
        return primaryKey;
    }

    /**
     * 获得值。
     * @return
     */
    public byte[] getValue() {
        //返回行的字节流。
        return columns[0];
    }

}
