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

package io.dingodb.net;

import java.util.ServiceLoader;

/**
 * mysql net service 提供者。
 */
public interface MysqlNetServiceProvider {
    /**
     * 获得默认的mysql服务提供者实例。
     * Get default net service provider impl.
     */
    static MysqlNetServiceProvider getDefault() {
        return ServiceLoader.load(MysqlNetServiceProvider.class).iterator().next();
    }

    /**
     * 获得mysql网络服务的实现接口。
     * Get net service instance.
     */
    MysqlNetService get();
}
