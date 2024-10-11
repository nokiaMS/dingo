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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * show collation 命令实现函数。
 *
 * mysql> show collation;
 * +-----------+---------+------+---------+----------+---------+
 * | Collation | Charset | Id   | Default | Compiled | Sortlen |
 * +-----------+---------+------+---------+----------+---------+
 * | utf8_bin  | utf8    | 83   |         | Yes      | 1       |
 * +-----------+---------+------+---------+----------+---------+
 * 1 row in set (0.01 sec)
 *
 * mysql>
 */
public class ShowCollationOperation extends QueryOperation {

    private String sqlLikePattern;

    /**
     * 构造函数。
     * @param sqlLikePattern
     */
    public ShowCollationOperation(String sqlLikePattern) {
        this.sqlLikePattern = sqlLikePattern;
    }

    /**
     * 结果集迭代器。
     * @return
     */
    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> tupleList = new ArrayList<>();
        //构造结果集。
        tupleList.add(new Object[] {"utf8_bin", "utf8", 83, "", "Yes", 1});
        return tupleList.iterator();
    }

    /**
     * 结果集元信息。
     * @return
     */
    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Collation");
        columns.add("Charset");
        columns.add("Id");
        columns.add("Default");
        columns.add("Compiled");
        columns.add("Sortlen");
        return columns;
    }
}
