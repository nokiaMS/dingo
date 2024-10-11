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

package io.dingodb.store.proxy;

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Set;

/**
 * 此Configuration对象对应了执行器配置文件中的store字段。
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Configuration {

    public static final String KEY = "store";

    /**
     * 配置实例，返回的是 DingoConfiguration 对象。
     *      其返回的是配置文件中的store段。
     */
    private static final Configuration INSTANCE = DingoConfiguration.instance().getConfig(KEY, Configuration.class);

    /**
     * 返回配置对象实例（即配置文件中的store段。）
     * @return
     */
    public static Configuration instance() {
        return INSTANCE;
    }

    /**
     * 配置文件中的coordinators字段。
     */
    private String coordinators;

    /**
     * 配置文件中coordinators字段解析后形成的集合结构。
     */
    private Set<Location> coordinatorSet;

    /**
     * 从配置文件中获取coordinators字段并返回。
     * @return
     */
    public static String coordinators() {
        if (INSTANCE.coordinators == null) {
            INSTANCE.coordinators = DingoConfiguration.instance().find("coordinators", String.class);
        }
        return INSTANCE.coordinators;
    }

    /**
     * 获取coordinator集合。
     * @return
     */
    public static Set<Location> coordinatorSet() {
        if (INSTANCE.coordinatorSet == null) {
            INSTANCE.coordinatorSet = Services.parse(coordinators());
        }
        return INSTANCE.coordinatorSet;
    }
}
