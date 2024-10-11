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

package io.dingodb.common.ddl;

import java.util.function.Function;

/**
 * ddl job事件监听器实现类。
 */
public class DdlJobListenerImpl implements DdlJobListener {
    /**
     * 事件响应函数。
     */
    private final Function<DdlJobEvent, Boolean> function;

    /**
     * 构造函数。
     * @param function  响应函数。
     */
    public DdlJobListenerImpl(Function<DdlJobEvent, Boolean> function) {
        this.function = function;
    }

    /**
     * 事件处理函数。
     * @param event 事件。
     */
    @Override
    public void eventOccurred(DdlJobEvent event) {
        //应用事件响应函数处理事件。
        function.apply(event);
    }
}
