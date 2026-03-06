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

package io.dingodb.calcite.stats.utils;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public final class StatsMonitor {
    public static final StatsMonitor INSTANCE = new StatsMonitor();

    private final BlockingQueue<StatsTableModifyInfo> statsInfoQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Long, TableCount> statsMergedInfo = new ConcurrentHashMap<>();

    public void init() {
        statsInfoQueueHandle();
    }

    public void statsInfoQueueHandle() {
        if (statsInfoQueue == null || statsMergedInfo == null) {
            return;
        }
        try {
            // Block until a stats item is available, then merge immediately.
            StatsTableModifyInfo info = statsInfoQueue.take();
            Iterator<TableCount> iterator = info.iterator();
            while (iterator.hasNext()) {
                TableCount incoming = iterator.next();
                if (incoming == null) {
                    continue;
                }
                statsMergedInfo.compute(incoming.getTableId(), (key, existing) -> {
                    if (existing == null) {
                        TableCount created = new TableCount();
                        created.setTableId(incoming.getTableId());
                        created.setTableName(incoming.getTableName());
                        created.setCount(incoming.getCount());
                        return created;
                    }
                    existing.setCount(existing.getCount() + incoming.getCount());
                    if (existing.getTableName() == null && incoming.getTableName() != null) {
                        existing.setTableName(incoming.getTableName());
                    }
                    return existing;
                });
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
