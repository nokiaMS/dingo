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
 * show charset; 命令实现函数。
 *
 * mysql> show charset;
 * +---------+---------------+-------------------+--------+
 * | Charset | Description   | Default collation | Maxlen |
 * +---------+---------------+-------------------+--------+
 * | utf8    | UTF-8 Unicode | utf8_bin          | 3      |
 * +---------+---------------+-------------------+--------+
 * 1 row in set (0.02 sec)
 *
 * mysql>
 */
public class ShowCharsetOperation extends QueryOperation {

    private String sqlLikePattern;

    /**
     * 构造函数。
     * @param sqlLikePattern
     */
    public ShowCharsetOperation(String sqlLikePattern) {
        this.sqlLikePattern = sqlLikePattern;
    }

    /**
     * 结果集迭代器。
     * @return
     */
    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> tupleList = new ArrayList<>();
        //构造返回结果集。
        tupleList.add(new Object[] {"utf8", "UTF-8 Unicode", "utf8_bin", 3});
        return tupleList.iterator();
    }

    /**
     * 结果集元信息（列名称列表）
     * @return
     */
    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Charset");         //字符集名称。
        columns.add("Description");     //字符集描述。
        columns.add("Default collation");   //默认字符集。
        columns.add("Maxlen");          //字符集最大长度。
        return columns;
    }
}
