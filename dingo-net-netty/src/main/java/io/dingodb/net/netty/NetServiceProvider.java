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

package io.dingodb.net.netty;

import com.google.auto.service.AutoService;

/**
 * @AutoService是Google开源的一个小插件，它可以自动生成META-INF/services的配置文件，从而避免了手动创建配置文件的步骤。
 * 在注解处理器注册中，它简化了SPI配置过程，使得注解处理器的注册更为便捷。
 */
@AutoService(io.dingodb.net.NetServiceProvider.class)
public class NetServiceProvider implements io.dingodb.net.NetServiceProvider {

    /**
     * 一个静态对象，即一个NetService对象的单例。
     */
    public static final NetService NET_SERVICE_INSTANCE = new NetService();

    /**
     * 实现了get接口。
     * @return 返回一个NetService对象。
     */
    @Override
    public io.dingodb.net.NetService get() {
        return NET_SERVICE_INSTANCE;    //返回单例。
    }

    /**
     * 创建一个新的NetService对象。
     * @return NetService对象。
     */
    public io.dingodb.net.NetService newService() {
        return new NetService();
    }

}
