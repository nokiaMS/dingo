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

package io.dingodb.common.mysql;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;

/**
 * MySQLMessage provides mysql byte operation.
 * mysql消息格式。
 *
 * @author
 */
public class MysqlMessage {
    public static final long NULL_LENGTH = -1;

    /**
     * 空字节流，当消息已经读取完毕，没有更多字节的时候返回此消息。
     */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * 最原始的消息字节流。
     */
    private final byte[] data;

    /**
     * 消息总长度。
     */
    private final int length;

    /**
     * 消息当前的待读取位置。
     */
    private int position;

    /**
     * 构造函数。
     * @param data
     */
    public MysqlMessage(byte[] data) {
        this.data = data;
        this.length = data.length;
        this.position = 0;
    }

    public int length() {
        return length;
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = position;
    }

    public byte[] bytes() {
        return data;
    }

    public void move(int index) {
        position += index;
    }

    public boolean hasRemaining() {
        return length > position;
    }

    public byte read(int index) {
        return data[index];
    }

    /**
     * 从消息中读取一个字节并返回。
     * @return
     */
    public byte read() {
        //从buffer中返回一个字节。
        return data[position++];
    }

    public int readUB2() {
        final byte[] b = this.data;
        int i = b[position++] & 0xff;
        i |= (b[position++] & 0xff) << 8;
        return i;
    }

    public int readUB3() {
        final byte[] b = this.data;
        int i = b[position++] & 0xff;
        i |= (b[position++] & 0xff) << 8;
        i |= (b[position++] & 0xff) << 16;
        return i;
    }

    public long readUB4() {
        final byte[] b = this.data;
        long l = (long) (b[position++] & 0xff);
        l |= (long) (b[position++] & 0xff) << 8;
        l |= (long) (b[position++] & 0xff) << 16;
        l |= (long) (b[position++] & 0xff) << 24;
        return l;
    }

    public int readInt() {
        final byte[] b = this.data;
        int i = b[position++] & 0xff;
        i |= (b[position++] & 0xff) << 8;
        i |= (b[position++] & 0xff) << 16;
        i |= (b[position++] & 0xff) << 24;
        return i;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public long readLong() {
        final byte[] b = this.data;
        long l = (long) (b[position++] & 0xff);
        l |= (long) (b[position++] & 0xff) << 8;
        l |= (long) (b[position++] & 0xff) << 16;
        l |= (long) (b[position++] & 0xff) << 24;
        l |= (long) (b[position++] & 0xff) << 32;
        l |= (long) (b[position++] & 0xff) << 40;
        l |= (long) (b[position++] & 0xff) << 48;
        l |= (long) (b[position++] & 0xff) << 56;
        return l;
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public long readLength() {
        int length = data[position++] & 0xff;
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2();
            case 253:
                return readUB3();
            case 254:
                return readLong();
            default:
                return length;
        }
    }

    /**
     * 返回消息中剩余的所有未读部分。
     * @return  消息中所以未读部分。
     */
    public byte[] readBytes() {
        //如果已经读取完毕，那么返回空字节数组。
        if (position >= length) {
            return EMPTY_BYTES;
        }

        //获得剩余消息字节流。
        byte[] ab = new byte[length - position];
        System.arraycopy(data, position, ab, 0, ab.length);
        //更新当前读取到的消息为止。
        position = length;

        //返回字节流数组。
        return ab;
    }

    public byte[] readBytes(int length) {
        byte[] ab = new byte[length];
        System.arraycopy(data, position, ab, 0, length);
        position += length;
        return ab;
    }

    public byte[] readBytesWithNull() {
        final byte[] b = this.data;
        if (position >= length) {
            return EMPTY_BYTES;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (b[i] == 0) {
                offset = i;
                break;
            }
        }
        switch (offset) {
            case -1:
                byte[] ab1 = new byte[length - position];
                System.arraycopy(b, position, ab1, 0, ab1.length);
                position = length;
                return ab1;
            case 0:
                position++;
                return EMPTY_BYTES;
            default:
                byte[] ab2 = new byte[offset - position];
                System.arraycopy(b, position, ab2, 0, ab2.length);
                position = offset + 1;
                return ab2;
        }
    }

    public byte[] readBytesWithLength() {
        int length = (int) readLength();
        if (length == NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        byte[] ab = new byte[length];
        System.arraycopy(data, position, ab, 0, ab.length);
        position += length;
        return ab;
    }

    public String readString() {
        if (position >= length) {
            return null;
        }
        String s = new String(data, position, length - position);
        position = length;
        return s;
    }

    public String readString(String charset)
        throws UnsupportedEncodingException {
        if (position >= length) {
            return null;
        }
        String s = new String(data, position, length - position, charset);
        position = length;
        return s;
    }

    public String readStringWithNull() {
        final byte[] b = this.data;
        if (position >= length) {
            return null;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (b[i] == 0) {
                offset = i;
                break;
            }
        }
        if (offset == -1) {
            String s = new String(b, position, length - position);
            position = length;
            return s;
        }
        if (offset > position) {
            String s = new String(b, position, offset - position);
            position = offset + 1;
            return s;
        } else {
            position++;
            return null;
        }
    }

    public String readStringWithNull(String charset)
        throws UnsupportedEncodingException {
        final byte[] b = this.data;
        if (position >= length) {
            return null;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (b[i] == 0) {
                offset = i;
                break;
            }
        }
        switch (offset) {
            case -1:
                String s1 = new String(b, position, length - position, charset);
                position = length;
                return s1;
            case 0:
                position++;
                return null;
            default:
                String s2 = new String(b, position, offset - position, charset);
                position = offset + 1;
                return s2;
        }
    }

    public String readStringWithLength() {
        int length = (int) readLength();
        if (length <= 0) {
            return null;
        }
        String s = new String(data, position, length);
        position += length;
        return s;
    }

    public String readStringWithLength(String charset)
        throws UnsupportedEncodingException {
        int length = (int) readLength();
        if (length <= 0) {
            return null;
        }
        String s = new String(data, position, length, charset);
        position += length;
        return s;
    }

    public Time readTime() {
        move(6);
        int hour = read();
        int minute = read();
        int second = read();
        Calendar cal = getLocalCalendar();
        cal.set(0, 0, 0, hour, minute, second);
        return new Time(cal.getTimeInMillis());
    }

    public java.util.Date readDate() {
        byte length = read();
        int year = readUB2();
        byte month = read();
        byte date = read();
        int hour = read();
        int minute = read();
        int second = read();
        if (length == 11) {
            long nanos = readUB4();
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            Timestamp time = new Timestamp(cal.getTimeInMillis());
            time.setNanos((int) nanos);
            return time;
        } else {
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            return new java.sql.Date(cal.getTimeInMillis());
        }
    }

    public BigDecimal readBigDecimal() {
        String src = readStringWithLength();
        return src == null ? null : new BigDecimal(src);
    }

    public String toString() {
        return new StringBuilder().append(Arrays.toString(data)).toString();
    }

    private static final ThreadLocal<Calendar> localCalendar = new ThreadLocal<Calendar>();

    private static final Calendar getLocalCalendar() {
        Calendar cal = localCalendar.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            localCalendar.set(cal);
        }
        return cal;
    }

}
