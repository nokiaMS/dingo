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

package io.dingodb.meta.entity;

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 元信息缓存：
 *      存储InfoSchema对象，其内部存储实现为一个数组。
 *      包含了一个读写锁，在写入及读取的时候会对cache对象进行加锁。
 *      在cache中，is按照版本号顺序排列，越靠近数组左边的is，其版本号越大。
 */
@Slf4j
public class InfoCache {
    /**
     * 创建info cache单例。
     */
    public static final InfoCache infoCache = new InfoCache(16);

    /**
     * 读写锁。
     */
    ReentrantReadWriteLock lock;

    /**
     * InfoSchema数组。
     */
    public InfoSchema[] cache;

    /**
     * 最大快照更新时间戳。
     */
    long maxUpdatedSnapshotTS;

    /**
     * 构造函数。
     * @param capacity  cache长度。
     */
    private InfoCache(int capacity) {
        lock = new ReentrantReadWriteLock();
        this.cache = new InfoSchema[capacity];
    }

    /**
     * 获得最新的InfoSchema对象。
     * @return
     */
    public InfoSchema getLatest() {
        //加读锁。
        lock.readLock().lock();
        try {
            if (cache.length > 0) {
                //cache中的is按照版本号排列，最左边版本号最大，即is最新，因此cache[0]是最新的is。
                return cache[0];
            }
            return null;
        } finally {
            //释放读锁。
            lock.readLock().unlock();
        }
    }

    /**
     * 充值cache。
     * @param capacity cache大小。
     */
    public void reset(int capacity) {
        //加写锁。
        lock.writeLock().lock();
        try {
            //重新分配cache空间。
            this.cache = new InfoSchema[capacity];
        } finally {
            //释放写锁。
            lock.writeLock().unlock();
        }
    }

    /**
     * 根据版本号获得is对象。
     * @param version   is版本号。
     * @return
     */
    public InfoSchema getByVersion(long version) {
        lock.readLock().lock();
        try {
            int length = cache.length;
            int ix = getIsIndex(version, length);
            if (ix == -1) {
                return null;
            }
            if (ix < length && (ix != 0 || cache[ix].getSchemaMetaVersion() == version)) {
                return cache[ix];
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 根据is的版本号计算is在cache中的位置。
     * @param version   is版本号。
     * @param length    cache长度。
     * @return
     */
    private int getIsIndex(long version, int length) {
        int ix = -1;
        for (int i = 0; i < length; i ++) {
            InfoSchema infoSchema = cache[i];
            if (infoSchema == null) {
                continue;
            }
            //由此可见，在cache中，版本号是从左到右递减的。越靠近左边，版本号越大。
            if (infoSchema.getSchemaMetaVersion() <= version) {
                ix = i;
                break;
            }
        }
        return ix;
    }

    /**
     * 向cache中插入InfoSchema对象。
     * @param is     InfoSchema对象。
     * @param snapshotTs    快照时间戳。
     * @return
     */
    public boolean insert(InfoSchema is, long snapshotTs) {
        //对象为空则直接返回。
        if (is == null) {
            return false;
        }
//        if (is.schemaMap.containsKey("DINGO")) {
//            int size = is.schemaMap.get("DINGO").getTables().size();
//            LogUtils.info(log, "is dingo table size:{}, schemaVer:{}", size, is.schemaMetaVersion);
//        }

        //加写锁。
        lock.writeLock().lock();
        //获得当前is的版本号。
        long version = is.getSchemaMetaVersion();
        try {
            //计算is应该在cache中的插入位置。
            int ix = getIsIndex(version, cache.length);
            //LogUtils.info(log, "is insert before schemaVer:{}, get index:{}, cache size:{}", version, ix, getCacheCount());
            //如果没有此版本那么位置为0。
            if (ix == -1) {
                ix = 0;
            }

            //更新最大时间戳。
            if (this.maxUpdatedSnapshotTS < snapshotTs) {
                this.maxUpdatedSnapshotTS = snapshotTs;
            }

            //如果cache中已经存在了对应版本的is，那么不重新插入直接返回。
            if (ix < cache.length && this.cache[ix] != null) {
                if (this.cache[ix].getSchemaMetaVersion() == version) {
                    return true;
                }
            }

            //把is对象插入到cache的指定位置中。
            int len = getCacheCount();
            if (ix < len || len < cache.length) {
                // has free space, grown the slice
                for (int i = cache.length - 1; i > ix; i --) {
                    cache[i] = cache[i - 1];
                }
                cache[ix] = is;
                return true;
            }
            return false;
        } finally {
            //解除写锁。
            lock.writeLock().unlock();
            //LogUtils.info(log, "is insert after schemaVer:{}, cache size:{}", version, getCacheCount());
        }
    }

    /**
     * 获得cache中对象数量。
     * @return
     */
    private int getCacheCount() {
        int i = 0;
        for (InfoSchema infoSchema : cache) {
            if (infoSchema != null) {
                i ++;
            }
        }
        return i;
    }
}
