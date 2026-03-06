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

import static org.assertj.core.api.Assertions.assertThat;

public class TestTableCount {

    @Test
    public void testGetAndSet() {
        TableCount tableCount = new TableCount();

        tableCount.setTableId(42L);
        tableCount.setTableName("t_order");
        tableCount.setCount(9L);

        assertThat(tableCount.getTableId()).isEqualTo(42L);
        assertThat(tableCount.getTableName()).isEqualTo("t_order");
        assertThat(tableCount.getCount()).isEqualTo(9L);
    }

    @Test
    public void testDefaultValues() {
        TableCount tableCount = new TableCount();

        assertThat(tableCount.getTableId()).isEqualTo(0L);
        assertThat(tableCount.getTableName()).isNull();
        assertThat(tableCount.getCount()).isEqualTo(0L);
    }
}
