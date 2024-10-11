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

package io.dingodb.store.api.transaction.data;

/**
 * 事务操作的操作码：
 *      表示store需要执行的操作。
 */
public enum Op {
    NONE(0),
    PUT(1),
    DELETE(2),
    PUTIFABSENT(3),
    ROLLBACK(4),
    LOCK(5),
    CheckNotExists(6);

    private final int code;

    /**
     * 构造函数。
     * @param code
     */
    Op(int code) {
        this.code = code;
    }

    /**
     * 获得操作码。
     * @return
     */
    public int getCode() {
        return code;
    }

    /**
     * 判断是否为none操作。
     * @param code
     * @return
     */
    public static boolean isNone(int code) {
        return code == NONE.getCode();
    }

    /**
     * 判断是否为put操作。
     * @param code
     * @return
     */
    public static boolean isPut(int code) {
        return code == PUT.getCode();
    }

    /**
     * 判断是否为delete操作。
     * @param code
     * @return
     */
    public static boolean isDelete(int code) {
        return code == DELETE.getCode();
    }

    /**
     * 判断是否为不存在则添加操作。
     * @param code
     * @return
     */
    public static boolean isPutIfAbsent(int code) {
        return code == PUTIFABSENT.getCode();
    }

    /**
     * 判断是否为回滚操作。
     * @param code
     * @return
     */
    public static boolean isRollBack(int code) {
        return code == ROLLBACK.getCode();
    }

    /**
     * 判断是否为锁操作。
     * @param code
     * @return
     */
    public static boolean isLock(int code) {
        return code == LOCK.getCode();
    }

    /**
     * 根据操作码返回对应枚举值。
     * @param code
     * @return
     */
    public static Op forNumber(int code) {
        switch (code) {
            case 0: return NONE;
            case 1: return PUT;
            case 2: return DELETE;
            case 3: return PUTIFABSENT;
            case 4: return ROLLBACK;
            case 5: return LOCK;
            case 6: return CheckNotExists;
            default: return null;
        }
    }
}
