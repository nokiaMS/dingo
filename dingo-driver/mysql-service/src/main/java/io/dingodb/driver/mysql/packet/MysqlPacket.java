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

package io.dingodb.driver.mysql.packet;

import io.netty.buffer.ByteBuf;

/**
 * mysql响应包都继承于此结构。
 */
public abstract class MysqlPacket {

    // utf8mb4;
    public static final short charsetNumber = 45;
    public static final byte decimals = 0x00;

    /**
     * 包长度。
     */
    public int packetLength;

    /**
     * 包序号（唯一且自增。）
     */
    public byte packetId;

    /**
     * 返回包长度。
     * @return
     */
    public abstract int calcPacketSize();

    /**
     * 获得包信息。
     * @return
     */
    protected abstract String getPacketInfo();

    /**
     * 从包中读取数据存放到data中。
     * @param data
     */
    public abstract void read(byte[] data);

    /**
     * 向buffer中写入包。
     * @param buffer
     */
    public abstract void write(ByteBuf buffer);

    /**
     * 包序列化。
     * @return
     */
    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()).append("{length=")
            .append(packetLength).append(",id=").append(packetId)
            .append('}').toString();
    }

}
