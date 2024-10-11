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

package io.dingodb.calcite.operation;

import io.dingodb.common.profile.ExecProfile;

import java.util.Iterator;
import java.util.List;

/**
 * 定义 QueryOperation 抽象类，即数据库查询操作的接口，表示“查询操作”。
 * 继承类需要实现两个接口：
 *      getIterator() - 返回结果数据。
 *      columns() - 返回列标签。
 */
public abstract class QueryOperation implements Operation {

    public ExecProfile execProfile;

    /**
     * 返回结果数据集的迭代器。
     * @return
     */
    public Iterator<Object[]> iterator() {
        Iterator<Object[]> iterator = getIterator();
        this.execProfile.end();
        return iterator;
    }

    /**
     * 获得结果数据集的迭代器，即展示结果中各行与各列的值。
     * @return
     */
    abstract Iterator<Object[]> getIterator();

    /**
     * 定义类返回结果中的列名称信息，即展示结果中各列名称。
     * @return
     */
    public abstract List<String> columns();

    public void initExecProfile(ExecProfile execProfile) {
        this.execProfile = execProfile;
    }
}
