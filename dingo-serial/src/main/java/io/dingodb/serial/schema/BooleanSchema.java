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

/**
 * bool类型元信息定义。
 */
public class BooleanSchema implements DingoSchema {
    /**
     * 在tuple中的位置编号。
     */
    private int index;

    /**
     * 是否允许为null。
     */
    private boolean notNull;

    /**
     * 默认值。
     */
    private Boolean defaultValue;

    /**
     * 构造函数。
     * @param index     tuple中的位置编号。
     */
    public BooleanSchema(int index) {
        setIndex(index);
        setNotNull(false);
    }

    /**
     * 构造函数。
     * @param index     在tuple中的位置信息。
     * @param defaultValue  默认值。
     */
    public BooleanSchema(int index, Object defaultValue) {
        setIndex(index);
        setNotNull(true);
        setDefaultValue(defaultValue);
    }

    /**
     * 获得schema对应的类型。
     * @return
     */
    @Override
    public Type getType() {
        return Type.BOOLEAN;
    }

    /**
     * 设置位置编号字段。
     * @param index
     */
    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    /**
     * 获得在tuple中对应的位置值。
     * @return
     */
    @Override
    public int getIndex() {
        return index;
    }

    /**
     * 设置长度，bool类型长度固定，不支持设置长度。
     * @param length
     */
    @Override
    public void setLength(int length) {
        throw new UnsupportedOperationException("Boolean Schema data length always be 2");
    }

    /**
     * 获得类型的长度，此长度是对此类型进行编码时占用的字节数。bool占用两个字节，byte1为is_null，byte2为bool值。
     * @return
     */
    @Override
    public int getLength() {
        return 2;
    }

    /**
     * 设置类型的最大长度。
     * @param maxLength
     */
    @Override
    public void setMaxLength(int maxLength) {
        throw new UnsupportedOperationException("Boolean Schema data length always be 2");
    }

    /**
     * 获得类型的最大长度。
     * @return
     */
    @Override
    public int getMaxLength() {
        return 2;
    }

    /**
     * 设置标度，bool类型不支持设置标度。
     * @param precision
     */
    @Override
    public void setPrecision(int precision) {
        throw new UnsupportedOperationException("Boolean Schema not support Precision");
    }

    /**
     * 获得标度。
     * @return
     */
    @Override
    public int getPrecision() {
        return 0;
    }

    /**
     * 设置精度，bool类型不支持设置精度。
     * @param scale
     */
    @Override
    public void setScale(int scale) {
        throw new UnsupportedOperationException("Boolean Schema not support Scale");
    }

    /**
     * 获得精度，bool没有精度，返回0.
     * @return
     */
    @Override
    public int getScale() {
        return 0;
    }

    /**
     * 设置notNull字段。
     * @param notNull
     */
    @Override
    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    /**
     * 检测是否为null。
     * @return
     */
    @Override
    public boolean isNotNull() {
        return notNull;
    }

    /**
     * 设置默认值。
     * @param defaultValue
     * @throws ClassCastException
     */
    @Override
    public void setDefaultValue(Object defaultValue) throws ClassCastException {
        this.defaultValue = (Boolean) defaultValue;
    }

    /**
     * 获得默认值。
     * @return
     */
    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }
}
