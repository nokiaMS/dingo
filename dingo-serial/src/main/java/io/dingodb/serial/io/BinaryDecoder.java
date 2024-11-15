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

package io.dingodb.serial.io;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 二进制解码。
 */
public class BinaryDecoder {
    private final byte[] buf;
    private int forwardPosition = 0;
    private int reversePosition;

    /**
     * 构建二进制解码器。
     * @param buf
     */
    public BinaryDecoder(byte[] buf) {
        //工作区。
        this.buf = buf;
        this.reversePosition = buf.length - 1;
    }

    /**
     * 解码bool类型。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Boolean readBoolean() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition++;
            return null;
        }
        return buf[forwardPosition++] != 0;
    }

    /**
     * 解码short类型。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Short readShort() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 2;
            return null;
        }
        return (short) (((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    /**
     * 解码short key值。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Short readKeyShort() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 2;
            return null;
        }
        return (short) (((buf[forwardPosition++] & 0xFF ^ 0x80) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    /**
     * 读取int值。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Integer readInt() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 4;
            return null;
        }
        return (((buf[forwardPosition++] & 0xFF) << 24)
            | ((buf[forwardPosition++] & 0xFF) << 16)
            | ((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    /**
     * 读取int key值。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Integer readKeyInt() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 4;
            return null;
        }
        return (((buf[forwardPosition++] & 0xFF ^ 0x80) << 24)
            | ((buf[forwardPosition++] & 0xFF) << 16)
            | ((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    /**
     * 读取float值。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Float readFloat() throws IndexOutOfBoundsException {
        Integer i = readInt();
        if (i == null) {
            return null;
        }
        return Float.intBitsToFloat(i);
    }

    /**
     * 读取float key值。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Float readKeyFloat() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 4;
            return null;
        }
        int i = 0;
        if ((buf[forwardPosition] & 0xFF) >= 0x80) {
            i = (((buf[forwardPosition++] & 0xFF ^ 0x80) << 24)
                | ((buf[forwardPosition++] & 0xFF) << 16)
                | ((buf[forwardPosition++] & 0xFF) << 8)
                | buf[forwardPosition++] & 0xFF);
        } else {
            i = (((~buf[forwardPosition++] & 0xFF) << 24)
                | ((~buf[forwardPosition++] & 0xFF) << 16)
                | ((~buf[forwardPosition++] & 0xFF) << 8)
                | ~buf[forwardPosition++] & 0xFF);
        }
        return Float.intBitsToFloat(i);
    }

    /**
     * 读取long.
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Long readLong() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 8;
            return null;
        }
        long l = 0;
        for (int i = 0; i < 8; i++) {
            l <<= 8;
            l |= buf[forwardPosition++] & 0xFF;
        }
        return l;
    }

    /**
     * 读取 key long。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Long readKeyLong() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 8;
            return null;
        }
        long l = 0;
        l |= buf[forwardPosition++] & 0xFF ^ 0x80;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf[forwardPosition++] & 0xFF;
        }
        return l;
    }

    /**
     * 读取double。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Double readDouble() throws IndexOutOfBoundsException {
        Long l = readLong();
        if (l == null) {
            return null;
        }
        return Double.longBitsToDouble(l);
    }

    /**
     * 读取double key.
     * @return
     * @throws IndexOutOfBoundsException
     */
    public Double readKeyDouble() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            forwardPosition += 8;
            return null;
        }
        long l = 0;
        if ((buf[forwardPosition] & 0xFF) >= 0x80) {
            l |= buf[forwardPosition++] & 0xFF ^ 0x80;
            for (int i = 0; i < 7; i++) {
                l <<= 8;
                l |= buf[forwardPosition++] & 0xFF;
            }
        } else {
            for (int i = 0; i < 8; i++) {
                l <<= 8;
                l |= ~buf[forwardPosition++] & 0xFF;
            }
        }
        return Double.longBitsToDouble(l);
    }

    /**
     * 读取字节。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public byte[] readBytes() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int length = readLength();
            if (length > 0) {
                byte[] buf = new byte[length];
                System.arraycopy(this.buf, forwardPosition, buf, 0, length);
                skip(length);
                return buf;
            }
            return new byte[0];
        }
    }

    /**
     * 读取key字节。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public byte[] readKeyBytes() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int length = readKeyLength();
            if (length > 0) {
                int groupNum = length / 9;
                int reminderZero = 255 - buf[forwardPosition + length - 1] & 0xFF;
                int oriLength = groupNum * 8 - reminderZero;
                byte[] buf = new byte[oriLength];
                int bi = 0;
                buf[bi++] = this.buf[forwardPosition++];
                for (int i = 2; i < length - reminderZero; i++) {
                    if (i % 9 != 0) {
                        buf[bi++] = this.buf[forwardPosition++];
                    } else {
                        forwardPosition++;
                    }
                }
                forwardPosition += reminderZero;
                forwardPosition++;
                return buf;
            }
            return new byte[0];
        }
    }

    /**
     * 读取字符串。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public String readString() throws IndexOutOfBoundsException {
        byte[] buf = readBytes();
        if (buf == null) {
            return null;
        }
        if (buf.length == 0) {
            return "";
        }
        return new String(buf, StandardCharsets.UTF_8);
    }

    /**
     * 读取key字符串。
     * @return
     * @throws IndexOutOfBoundsException
     */
    public String readKeyString() throws IndexOutOfBoundsException {
        byte[] buf = readKeyBytes();
        if (buf == null) {
            return null;
        }
        if (buf.length == 0) {
            return "";
        }
        return new String(buf, StandardCharsets.UTF_8);
    }

    public List<Boolean> readBooleanList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<Boolean> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readBoolean());
            }
            return list;
        }
    }

    public List<Short> readShortList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<Short> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readShort());
            }
            return list;
        }
    }

    public List<Integer> readIntegerList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<Integer> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readInt());
            }
            return list;
        }
    }

    public List<Float> readFloatList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<Float> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readFloat());
            }
            return list;
        }
    }

    public List<Long> readLongList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<Long> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readLong());
            }
            return list;
        }
    }

    public List<Double> readDoubleList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<Double> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readDouble());
            }
            return list;
        }
    }

    public List<byte[]> readBytesList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<byte[]> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readBytes());
            }
            return list;
        }
    }

    public List<String> readStringList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readLength();
            List<String> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readString());
            }
            return list;
        }
    }

    public void skipByte() {
        skip(1);
    }

    public void skipBytes() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readLength());
        }
    }

    public void skipKeyBytes() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readKeyLength());
        }
    }

    public void skipString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readLength());
        }
    }

    public void skipKeyString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readKeyLength());
        }
    }

    public void skipBooleanList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(2 * readLength());
        }
    }

    public void skipShortList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(3 * readLength());
        }
    }

    public void skipIntegerList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readLength());
        }
    }

    public void skipFloatList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readLength());
        }
    }

    public void skipLongList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readLength());
        }
    }

    public void skipDoubleList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readLength());
        }
    }

    public void skipBytesList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            int size = readLength();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    skipBytes();
                }
            }
        }
    }

    public void skipStringList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            int size = readLength();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    skipString();
                }
            }
        }
    }

    public int remainder() {
        return buf.length - forwardPosition;
    }

    /**
     * 判断是否为null。
     * @return
     * @throws IndexOutOfBoundsException
     */
    private boolean readIsNull() throws IndexOutOfBoundsException {
        return buf[forwardPosition++] == 0;
    }

    private int readLength() throws IndexOutOfBoundsException {
        return (((buf[forwardPosition++] & 0xFF) << 24)
            | ((buf[forwardPosition++] & 0xFF) << 16)
            | ((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    private int readKeyLength() throws IndexOutOfBoundsException {
        return (((buf[reversePosition--] & 0xFF) << 24)
            | ((buf[reversePosition--] & 0xFF) << 16)
            | ((buf[reversePosition--] & 0xFF) << 8)
            | buf[reversePosition--] & 0xFF);
    }

    private void skipKeyLength() {
        reversePosition -= 4;
    }

    public void skip(int length) {
        forwardPosition += length;
    }
}
