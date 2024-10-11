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

package io.dingodb.common.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.util.ReflectionUtils.convert;

/**
 * dingo配置对象，代表了整个配置文件的结构。
 */
@Getter
@Setter
@ToString
@Slf4j
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DingoConfiguration {

    /**
     * yaml文件翻译器。
     */
    private static final ObjectMapper PARSER = YAMLMapper.builder().build();

    /**
     * 默认实例，单例。
     */
    private static final DingoConfiguration INSTANCE = new DingoConfiguration();

    /**
     * 从配置文件解析出来的kv键值对存储于此。
     */
    private final Map<String, Object> config = new ConcurrentHashMap<>();

    /**
     * server id。
     */
    private CommonId serverId;

    /**
     * @Delegate使得代理类具有了被代理类的方法。
     */
    @Delegate
    private ExchangeConfiguration exchange = new ExchangeConfiguration();   //exchange段配置。
    private SecurityConfiguration security = new SecurityConfiguration();   //security段配置。
    private VariableConfiguration variable = new VariableConfiguration();   //variable段配置。
    private CommonConfiguration common = new CommonConfiguration();         //配置文件对象。

    /**
     * 解析配置文件。
     * @param configPath    配置文件全路径。
     * @throws Exception
     */
    public static synchronized void parse(final String configPath) throws Exception {
        //解析配置文件并拷贝到config字段中。
        if (configPath != null) {
            INSTANCE.copyConfig(PARSER.readValue(new FileInputStream(configPath), Map.class), INSTANCE.config);
        }
        //从配置中获取对应字段并填充到配置对象对应字段中。
        INSTANCE.exchange = INSTANCE.getConfig("exchange", ExchangeConfiguration.class);
        INSTANCE.security = INSTANCE.getConfig("security", SecurityConfiguration.class);
        INSTANCE.variable = INSTANCE.getConfig("variable", VariableConfiguration.class);
        INSTANCE.common = INSTANCE.getConfig("common", CommonConfiguration.class);
    }

    /**
     * 把kv值从from拷贝到to中。
     * @param from
     * @param to
     */
    private static void copyConfig(Map<String, Object> from, Map<String, Object> to) {
        for (Map.Entry<String, Object> entry : from.entrySet()) {
            if (entry.getValue() instanceof Map) {
                copyConfig(
                    (Map<String, Object>) entry.getValue(),
                    (Map<String, Object>) to.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())    //to对象如果不存在则新建。
                );
            }
            to.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 获得配置对象实例。
     * @return
     */
    public static @NonNull DingoConfiguration instance() {
        return INSTANCE;
    }

    /**
     * 获得服务的host信息，通常是一个ip地址。
     * @return  返回配置文件中的host配置信息，是一个ip地址字符串。
     */
    public static String host() {
        return Optional.mapOrNull(INSTANCE.exchange, ExchangeConfiguration::getHost);
    }

    /**
     * 获得配置中的port信息。
     * @return  返回port值。
     */
    public static int port() {
        return Optional.mapOrGet(INSTANCE.exchange, ExchangeConfiguration::getPort, () -> 0);
    }

    /**
     * 获得scheduledCoreThreads字段。
     * @return
     */
    public static int scheduledCoreThreads() {
        return Optional.mapOrGet(INSTANCE.common, CommonConfiguration::getScheduledCoreThreads, () -> 16);
    }

    /**
     * 获得lockCoreThreads字段。
     * @return
     */
    public static int lockCoreThreads() {
        return Optional.mapOrGet(INSTANCE.common, CommonConfiguration::getLockCoreThreads, () -> 0);
    }

    /**
     * 获得globalCoreThreads字段。
     * @return
     */
    public static int globalCoreThreads() {
        return Optional.mapOrGet(INSTANCE.common, CommonConfiguration::getGlobalCoreThreads, () -> 0);
    }

    /**
     * 获得server id字段。
     * @return
     */
    public static CommonId serverId() {
        return INSTANCE.serverId;
    }

    /**
     * 读取配置中的host和port字段，生成localion，表示一个"位置"。
     * @return  返回Location对象。
     */
    public static @NonNull Location location() {
        //构造Location对象并返回。
        return new Location(host(), port());
    }

    /**
     * 给定key，查找并返回对应配置信息。
     * @param key
     * @param targetType
     * @return
     * @param <T>
     */
    public <T> T find(String key, Class<T> targetType) {
        return find(key, targetType, config);
    }

    /**
     * 给定key，查找并返回配置信息。
     * @param key
     * @param targetType
     * @param config
     * @return
     * @param <T>
     */
    private <T> T find(String key, Class<T> targetType, Map<String, Object> config) {
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getKey().equals(key)) {
                if (targetType.isInstance(entry.getValue())) {
                    return (T) entry.getValue();
                }
            }
            if (entry.getValue() instanceof Map) {
                T target = find(key, targetType, (Map<String, Object>) entry.getValue());
                if (target != null) {
                    return target;
                }
            }
        }
        return null;
    }

    /**
     * 从配置中读取key对应的value.
     * @param key
     * @param configType
     * @return
     * @param <T>
     */
    public <T> T getConfig(String key, Class<T> configType) {
        return convert(getConfigMap(key), configType);
    }

    /**
     * 返回key对应的配置段，如果不存在则新建。
     * @param key
     * @return
     */
    public Map<String, Object> getConfigMap(String key) {
        return (Map<String, Object>) INSTANCE.config.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
    }

}
