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
 * 网络服务发现提供者。
 */
public interface NetServiceProvider {

    /**
     * 返回默认的NetServiceProvider实例。
     * Get default net service provider impl.
     * ServiceLoader是java中SPI的一种实现，即服务发现机制。
     */
    static NetServiceProvider getDefault() {
        return ServiceLoader.load(NetServiceProvider.class).iterator().next();
    }

    /**
     * 此接口用于获得一个NetService实例。
     * Get net service instance.
     */
    NetService get();
}
