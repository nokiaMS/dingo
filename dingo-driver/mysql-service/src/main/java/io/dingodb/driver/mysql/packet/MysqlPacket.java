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

public abstract class MysqlPacket {

    // utf8mb4;
    public static final short charsetNumber = 45;
    public static final byte decimals = 0x00;

    public int packetLength;

    /**
     * 消息类型。
     */
    public byte packetId;

    public abstract int calcPacketSize();

    protected abstract String getPacketInfo();

    public abstract void read(byte[] data);

    public abstract void write(ByteBuf buffer);

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()).append("{length=")
            .append(packetLength).append(",id=").append(packetId)
            .append('}').toString();
    }

}
