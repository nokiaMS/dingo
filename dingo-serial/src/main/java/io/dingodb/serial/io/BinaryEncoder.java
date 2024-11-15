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
import java.util.Arrays;
import java.util.List;

/**
 * 二进制编码器。
 */
public class BinaryEncoder {
    /**
     * 二进制编码的存储位置(工作缓冲区)
     */
    private byte[] buf;
    private byte[] lengthBuf;
    private int forwardPosition = 0;
    private int reversePosition = 0;

    public BinaryEncoder(int initialCapacity) {
        this.buf = new byte[initialCapacity];
    }

    public BinaryEncoder(int forwardInitCap, int reverseInitCap) {
        this.buf = new byte[forwardInitCap];
        this.lengthBuf = new byte[reverseInitCap];
        this.reversePosition = reverseInitCap - 1;
    }

    /**
     * 构造函数。
     * @param buf
     */
    public BinaryEncoder(byte[] buf) {
        this.buf = buf;
    }

    public BinaryEncoder(byte[] buf, int reverseCap) {
        int forwardCap = buf.length - reverseCap;
        this.buf = new byte[forwardCap];
        this.lengthBuf = new byte[reverseCap];
        System.arraycopy(buf, 0, this.buf, 0, forwardCap);
        System.arraycopy(buf, forwardCap, this.lengthBuf, 0, reverseCap);
        this.reversePosition = reverseCap - 1;
    }

    public BinaryEncoder(byte[] forwardBuf, byte[] reverseBuf) {
        this.buf = forwardBuf;
        this.lengthBuf = reverseBuf;
        this.reversePosition = lengthBuf.length - 1;
    }

    /**
     * 向工作buf中写入一个字节。
     * @param b
     */
    public void write(byte b) {
        ensureRemainder(1);
        buf[forwardPosition++] = b;
    }

    /**
     * bool值的二进制编码：
     * 格式：
     *      byte1：null标志位；
     *      byte2：bool值：
     * 典型值：
     *      null：
     *          0x0 0x0
     *      true:
     *          0x1 0x1
     *      false:
     *          0x1 0x0
     *
     * @param bool
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeBoolean(Object bool) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(2);
        if (bool == null) {
            writeNull();
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Boolean) bool ? 1 : 0);
        }
    }

    /**
     * 写入short类型的值。
     * short类型占用3个字节， is_null | value.
     * 格式：
     *      byte1: is null标志位。
     *      byte2: short的高8 bits。
     *      byte3: short的低8 bits。
     *
     * @param sh
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeShort(Object sh) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(3);
        if (sh == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Short) sh >>> 8);     //无符号右移。
            buf[forwardPosition++] = (byte) ((Short) sh >>> 0);
        }
    }

    /**
     * short类型的key序列化函数。
     * @param sh
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeKeyShort(Object sh) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(3);
        if (sh == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Short) sh >>> 8 ^ 0x80);  //与非key有差异。
            buf[forwardPosition++] = (byte) ((Short) sh >>> 0);
        }
    }

    /**
     * int的序列化。
     * @param in
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeInt(Object in) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (in == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Integer) in >>> 24);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 16);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 8);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 0);
        }
    }

    /**
     * int key的序列化。
     * @param in
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeKeyInt(Object in) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (in == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Integer) in >>> 24 ^ 0x80);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 16);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 8);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 0);
        }
    }

    /**
     * 浮点数序列化。
     * @param fo
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeFloat(Object fo) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (fo == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            int in = Float.floatToIntBits((Float) fo);      //按照bits转换成int。
            buf[forwardPosition++] = (byte) (in >>> 24);
            buf[forwardPosition++] = (byte) (in >>> 16);
            buf[forwardPosition++] = (byte) (in >>> 8);
            buf[forwardPosition++] = (byte) in;
        }
    }

    /**
     * float key的序列化函数。
     * @param fo
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeKeyFloat(Object fo) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (fo == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            int in = (Float.floatToIntBits((Float) fo));        //按照bits转换成int值。
            if (in >= 0) {
                buf[forwardPosition++] = (byte) (in >>> 24 ^ 0x80);
                buf[forwardPosition++] = (byte) (in >>> 16);
                buf[forwardPosition++] = (byte) (in >>> 8);
                buf[forwardPosition++] = (byte) in;
            } else {
                buf[forwardPosition++] = (byte) (~ in >>> 24);
                buf[forwardPosition++] = (byte) (~ in >>> 16);
                buf[forwardPosition++] = (byte) (~ in >>> 8);
                buf[forwardPosition++] = (byte) ~ in;
            }
        }
    }

    /**
     * long的序列化。
     * @param ln
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeLong(Object ln) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (ln == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Long) ln >>> 56);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 48);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 40);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 32);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 24);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 16);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 8);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 0);
        }
    }

    /**
     * long key的序列化。
     * @param ln
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeKeyLong(Object ln) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (ln == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Long) ln >>> 56 ^ 0x80);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 48);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 40);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 32);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 24);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 16);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 8);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 0);
        }
    }

    /**
     * double的序列化。
     * @param dl
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeDouble(Object dl) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (dl == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            long ln = Double.doubleToLongBits((Double) dl);
            buf[forwardPosition++] = (byte) (ln >>> 56);
            buf[forwardPosition++] = (byte) (ln >>> 48);
            buf[forwardPosition++] = (byte) (ln >>> 40);
            buf[forwardPosition++] = (byte) (ln >>> 32);
            buf[forwardPosition++] = (byte) (ln >>> 24);
            buf[forwardPosition++] = (byte) (ln >>> 16);
            buf[forwardPosition++] = (byte) (ln >>> 8);
            buf[forwardPosition++] = (byte) ln;
        }
    }

    /**
     * double key的序列化。
     * @param dl
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeKeyDouble(Object dl) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (dl == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            long ln = (Double.doubleToLongBits((Double) dl));   //double转换成long。
            if (ln >= 0) {
                buf[forwardPosition++] = (byte) (ln >>> 56 ^ 0x80);
                buf[forwardPosition++] = (byte) (ln >>> 48);
                buf[forwardPosition++] = (byte) (ln >>> 40);
                buf[forwardPosition++] = (byte) (ln >>> 32);
                buf[forwardPosition++] = (byte) (ln >>> 24);
                buf[forwardPosition++] = (byte) (ln >>> 16);
                buf[forwardPosition++] = (byte) (ln >>> 8);
                buf[forwardPosition++] = (byte) ln;
            } else {
                buf[forwardPosition++] = (byte) (~ ln >>> 56);
                buf[forwardPosition++] = (byte) (~ ln >>> 48);
                buf[forwardPosition++] = (byte) (~ ln >>> 40);
                buf[forwardPosition++] = (byte) (~ ln >>> 32);
                buf[forwardPosition++] = (byte) (~ ln >>> 24);
                buf[forwardPosition++] = (byte) (~ ln >>> 16);
                buf[forwardPosition++] = (byte) (~ ln >>> 8);
                buf[forwardPosition++] = (byte) ~ ln;
            }
        }
    }

    /**
     * bytes的序列化。
     * @param bytes
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((byte[]) bytes).length == 0) {
            ensureRemainder(5);
            writeNotNull();
            writeLength(0);
        } else {
            internWriteBytes((byte[]) bytes);
        }
    }

    public void updateBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipBytes();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeBytes(bytes);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipBytes() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readLength());
        }
    }

    public void writeKeyBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((byte[]) bytes).length == 0) {
            ensureRemainder(1);
            writeNotNull();
            writeKeyLength(0);
        } else {
            internWriteKeyBytes((byte[]) bytes);
        }
    }

    public void updateKeyBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        final int startMark = forwardPosition;
        skipKeyBytes();
        reversePosition += 4;
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeKeyBytes(bytes);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipKeyBytes() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readKeyLength());
        }
    }

    /**
     * string序列化。
     * 结构：
     *      byte1：in_null（1）
     *      byte2:
     * @param string
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(5);
            writeNotNull();
            writeLength(0);
        } else {
            //获得指定编码下的字符串的字节序列。
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            //进行二进制编码。
            internWriteBytes(value);
        }
    }

    public void updateString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        final int startMark = forwardPosition;
        skipString();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeString(string);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readLength());
        }
    }

    public void writeKeyString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(1);
            writeNotNull();
            writeKeyLength(0);
        } else {
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            internWriteKeyBytes(value);
        }
    }

    public void updateKeyString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        final int startMark = forwardPosition;
        skipKeyString();
        reversePosition += 4;
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeKeyString(string);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipKeyString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readKeyLength());
        }
    }

    /**
     * bool list编码。
     * @param booleanList
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void writeBooleanList(Object booleanList) throws IndexOutOfBoundsException, ClassCastException {
        if (booleanList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Boolean> list = (List<Boolean>) booleanList;
            ensureRemainder(5 + (2 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeBoolean(list.get(i));
            }
        }
    }

    /**
     * 更新bool list的编码值。
     * @param booleanList
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public void updateBooleanList(Object booleanList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipBooleanList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeBooleanList(booleanList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipBooleanList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(2 * readLength());
        }
    }

    public void writeShortList(Object shortList) throws IndexOutOfBoundsException, ClassCastException {
        if (shortList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Short> list = (List<Short>) shortList;
            ensureRemainder(5 + (3 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeShort(list.get(i));
            }
        }
    }

    public void updateShortList(Object shortList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipShortList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeShortList(shortList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipShortList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(3 * readLength());
        }
    }

    public void writeIntegerList(Object integerList) throws IndexOutOfBoundsException, ClassCastException {
        if (integerList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Integer> list = (List<Integer>) integerList;
            ensureRemainder(5 + (5 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeInt(list.get(i));
            }
        }
    }

    public void updateIntegerList(Object integerList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipIntegerList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeIntegerList(integerList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipIntegerList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readLength());
        }
    }

    public void writeFloatList(Object floatList) throws IndexOutOfBoundsException, ClassCastException {
        if (floatList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Float> list = (List<Float>) floatList;
            ensureRemainder(5 + (5 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeFloat(list.get(i));
            }
        }
    }

    public void updateFloatList(Object floatList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipFloatList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeFloatList(floatList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipFloatList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readLength());
        }
    }

    public void writeLongList(Object longList) throws IndexOutOfBoundsException, ClassCastException {
        if (longList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Long> list = (List<Long>) longList;
            ensureRemainder(5 + (9 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeLong(list.get(i));
            }
        }
    }

    public void updateLongList(Object longList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipLongList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeLongList(longList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipLongList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readLength());
        }
    }

    public void writeDoubleList(Object doubleList) throws IndexOutOfBoundsException, ClassCastException {
        if (doubleList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Double> list = (List<Double>) doubleList;
            ensureRemainder(5 + (9 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeDouble(list.get(i));
            }
        }
    }

    public void updateDoubleList(Object doubleList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipDoubleList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeDoubleList(doubleList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipDoubleList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readLength());
        }
    }

    public void writeBytesList(Object bytesList) throws IndexOutOfBoundsException, ClassCastException {
        if (bytesList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<byte[]> list = (List<byte[]>) bytesList;
            ensureRemainder(5);
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeBytes(list.get(i));
            }
        }
    }

    public void updateBytesList(Object bytesList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipBytesList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeBytesList(bytesList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
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

    public void writeStringList(Object stringList) throws IndexOutOfBoundsException, ClassCastException {
        if (stringList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<String> list = (List<String>) stringList;
            ensureRemainder(5);
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeString(list.get(i));
            }
        }
    }

    public void updateStringList(Object stringList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipStringList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeStringList(stringList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
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

    /**
     * null值用0表示，写入null值。
     * @throws IndexOutOfBoundsException
     */
    private void writeNull() throws IndexOutOfBoundsException {
        buf[forwardPosition++] = 0;
    }

    /**
     * 非null值用1表示。
     * @throws IndexOutOfBoundsException
     */
    private void writeNotNull() throws IndexOutOfBoundsException {
        buf[forwardPosition++] = 1;
    }

    /**
     * 长度数值序列化，长度是个整型，按照整型序列化。
     * @param length
     * @throws IndexOutOfBoundsException
     */
    private void writeLength(int length) throws IndexOutOfBoundsException {
        buf[forwardPosition++] = (byte) (length >> 24);
        buf[forwardPosition++] = (byte) (length >> 16);
        buf[forwardPosition++] = (byte) (length >> 8);
        buf[forwardPosition++] = (byte) length;
    }

    /**
     * key长度的序列化。
     * @param length
     * @throws IndexOutOfBoundsException
     */
    private void writeKeyLength(int length) throws IndexOutOfBoundsException {
        lengthBuf[reversePosition--] = (byte) (length >>> 24);
        lengthBuf[reversePosition--] = (byte) (length >>> 16);
        lengthBuf[reversePosition--] = (byte) (length >>> 8);
        lengthBuf[reversePosition--] = (byte) length;
    }

    /**
     * bytes数组的序列化。
     * @param value
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    private void internWriteBytes(byte[] value) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5 + value.length);
        writeNotNull();
        writeLength(value.length);  //写入字符串长度字段。
        System.arraycopy(value, 0, buf, forwardPosition, value.length);
        forwardPosition += value.length;
    }

    /**
     * bytes key的序列化。
     * @param value
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    private void internWriteKeyBytes(byte[] value) throws IndexOutOfBoundsException, ClassCastException {
        int groupNum = value.length / 8;
        int size = (groupNum + 1) * 9;
        int reminderSize = value.length % 8;
        int remindZero;
        if (reminderSize == 0) {
            reminderSize = 8;
            remindZero = 8;
        } else {
            remindZero = 8 - reminderSize;
        }
        ensureRemainder(1 + size);
        writeNotNull();
        writeKeyLength(size);
        for (int i = 0; i < groupNum; i++) {
            System.arraycopy(value, 8 * i, buf, forwardPosition, 8);
            forwardPosition += 8;
            buf[forwardPosition++] = (byte) 255;
        }
        if (reminderSize < 8) {
            System.arraycopy(value, 8 * groupNum, buf, forwardPosition, reminderSize);
            forwardPosition += reminderSize;
        }
        for (int i = 0; i < remindZero; i++) {
            buf[forwardPosition++] = 0;
        }
        buf[forwardPosition++] = (byte) (255 - remindZero);
    }

    /**
     * 跳过一个byte。
     */
    public void skipByte() {
        skip(1);
    }

    /**
     * 判断是否为null。
     * @return
     * @throws IndexOutOfBoundsException
     */
    private boolean readIsNull() throws IndexOutOfBoundsException {
        return buf[forwardPosition++] == 0;
    }

    /**
     * 读取一个byte。
     * @return
     */
    public byte read() {
        return buf[forwardPosition++];
    }

    /**
     * 读取一个short。
     * @return
     * @throws IndexOutOfBoundsException
     * @throws ClassCastException
     */
    public Short readShort() throws IndexOutOfBoundsException, ClassCastException {
        if (readIsNull()) {
            forwardPosition += 2;
            return null;
        }
        return (short) (((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    /**
     * 读取长度。
     * @return
     * @throws IndexOutOfBoundsException
     */
    private int readLength() throws IndexOutOfBoundsException {
        return (((buf[forwardPosition++] & 0xFF) << 24)
            | ((buf[forwardPosition++] & 0xFF) << 16)
            | ((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    private int readKeyLength() throws IndexOutOfBoundsException {
        return (((lengthBuf[reversePosition--] & 0xFF) << 24)
            | ((lengthBuf[reversePosition--] & 0xFF) << 16)
            | ((lengthBuf[reversePosition--] & 0xFF) << 8)
            | lengthBuf[reversePosition--] & 0xFF);
    }

    private void skipKeyLength() {
        reversePosition -= 4;
    }

    /**
     * 跳过n个bytes。
     * @param length
     */
    public void skip(int length) {
        forwardPosition += length;
    }

    /**
     * 保证buf剩余的空间能够满足length，如果不能够满足则进行空间的扩充，重新分配内存。
     * @param length
     */
    private void ensureRemainder(int length) {
        if (buf.length - forwardPosition < length) {
            buf = Arrays.copyOf(buf, forwardPosition + length);
        }
    }

    /**
     * 获得序列化后的字节数组。
     * @return
     */
    public byte[] getByteArray() {
        if (lengthBuf != null && lengthBuf.length > 0) {
            ensureRemainder(lengthBuf.length);
            System.arraycopy(lengthBuf, 0, buf, forwardPosition, lengthBuf.length);
            forwardPosition += lengthBuf.length;
        }

        //只拷贝有效部分。
        if (forwardPosition != buf.length) {
            buf = Arrays.copyOf(buf, forwardPosition);
        }

        //换回序列化值。
        return buf;
    }

    public byte[] getByteArrayWithoutLength() {
        if (forwardPosition != buf.length) {
            buf = Arrays.copyOf(buf, forwardPosition);
        }
        return buf;
    }
}
