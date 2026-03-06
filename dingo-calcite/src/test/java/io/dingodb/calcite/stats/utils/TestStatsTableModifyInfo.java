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

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatsTableModifyInfo {

    @Test
    public void testAddTableCountMerges() {
        StatsTableModifyInfo info = new StatsTableModifyInfo();

        TableCount first = new TableCount();
        first.setTableId(1L);
        first.setTableName("t_order");
        first.setCount(2L);
        info.addTableCount(1L, first);

        TableCount second = new TableCount();
        second.setTableId(1L);
        second.setTableName("t_order");
        second.setCount(3L);
        info.addTableCount(1L, second);

        assertThat(info.getTableCount(1L)).isEqualTo(5L);
        assertThat(info.getTableCountMap()).hasSize(1);
    }

    @Test
    public void testAddTableModifyInfoCreates() {
        StatsTableModifyInfo info = new StatsTableModifyInfo();

        TableCount tableCount = new TableCount();
        tableCount.setTableId(2L);
        tableCount.setTableName("t_user");
        tableCount.setCount(7L);
        info.addTableModifyInfo(tableCount);

        assertThat(info.getTableCount(2L)).isEqualTo(7L);
        assertThat(info.getTableCountMap().get(2L).getTableName()).isEqualTo("t_user");
    }

    @Test
    public void testIteratorReturnsAllValues() {
        StatsTableModifyInfo info = new StatsTableModifyInfo();

        TableCount first = new TableCount();
        first.setTableId(10L);
        first.setCount(1L);
        info.addTableCount(10L, first);

        TableCount second = new TableCount();
        second.setTableId(11L);
        second.setCount(2L);
        info.addTableCount(11L, second);

        Iterator<TableCount> iterator = info.iterator();
        assertThat(iterator).toIterable().hasSize(2);
    }

    @Test
    public void testGetTableCountMissingReturnsZero() {
        StatsTableModifyInfo info = new StatsTableModifyInfo();
        assertThat(info.getTableCount(404L)).isEqualTo(0L);
    }
}
