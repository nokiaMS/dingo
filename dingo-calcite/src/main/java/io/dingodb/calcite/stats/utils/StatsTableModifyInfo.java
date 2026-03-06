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
import java.util.concurrent.ConcurrentHashMap;

public class StatsTableModifyInfo {
    private final ConcurrentHashMap<Long, TableCount> tableCountMap = new ConcurrentHashMap<>();

    public void addTableModifyInfo(TableCount tableCount) {
        if (tableCount == null) {
            return;
        }
        addTableCount(tableCount.getTableId(), tableCount);
    }

    public void addTableCount(long tableId, TableCount tableCount) {
        if (tableCount == null) {
            return;
        }
        tableCountMap.merge(tableId, tableCount, (oldValue, newValue) -> {
            oldValue.setCount(oldValue.getCount() + newValue.getCount());
            return oldValue;
        });
    }

    public ConcurrentHashMap<Long, TableCount> getTableCountMap() {
        return tableCountMap;
    }

    public long getTableCount(long tableId) {
        TableCount tableCount = tableCountMap.get(tableId);
        return tableCount == null ? 0L : tableCount.getCount();
    }

    public Iterator<TableCount> iterator() {
        return tableCountMap.values().iterator();
    }
}
