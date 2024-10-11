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

package io.dingodb.tso;

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 创建并返回TsoService对象。
 */
public interface TsoServiceProvider {

    /**
     * 定义了一个默认的TsoServiceProvider实现。
     */
    @Slf4j
    class Impl {
        /**
         * 创建了一个默认实例。
         */
        private static final Impl INSTANCE = new Impl();

        private final TsoServiceProvider serviceProvider;

        /**
         * 默认构造函数。
         */
        private Impl() {
            Iterator<TsoServiceProvider> iterator
                = ServiceLoader.load(TsoServiceProvider.class).iterator();
            this.serviceProvider = iterator.next();
            if (iterator.hasNext()) {
                LogUtils.warn(log, "Load multi tool service provider, use {}.", serviceProvider.getClass().getName());
            }
        }
    }

    TsoService get();

    /**
     * 返回默认的serviceProvider实例。
     * @return  默认的serviceProvider。
     */
    static TsoServiceProvider getDefault() {
        return Impl.INSTANCE.serviceProvider;
    }
}
