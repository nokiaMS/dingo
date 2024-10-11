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

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.TsoRequest;
import io.dingodb.sdk.service.entity.meta.TsoTimestamp;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.tso.TsoServiceProvider;

import java.util.Set;

import static io.dingodb.sdk.service.entity.meta.TsoOpType.OP_GEN_TSO;

/**
 * tso服务实现类。
 */
public class TsoService implements io.dingodb.tso.TsoService {

    /**
     * 默认实例。
     */
    public static final TsoService INSTANCE = new TsoService();

    /**
     * tso服务生成器。
     * @AutoService是Google开源的一个小插件，它可以自动生成META-INF/services的配置文件，
     * 从而避免了手动创建配置文件的步骤。在注解处理器注册中，它简化了SPI配置过程，使得注解处理器的注册更为便捷。
     */
    @AutoService(TsoServiceProvider.class)
    public static class Provider implements TsoServiceProvider {
        @Override
        public io.dingodb.tso.TsoService get() {
            return INSTANCE;
        }
    }

    /**
     * tso与时间戳转换时的位移。
     */
    private static final int PHYSICAL_SHIFT = 18;

    /**
     * 最大逻辑数值，此值用于填充tso的右侧18个bits。
     */
    private static final long MAX_LOGICAL = (1 << PHYSICAL_SHIFT) - 1;

    /**
     * tso元信息服务对象。
     */
    private MetaService tsoMetaService;

    /**
     * 构造函数。
     */
    public TsoService() {
        //从配置中获得coordinators信息。
        String coordinators = Configuration.coordinators();
        if (coordinators == null) {
            tsoMetaService = null;
            return;
        }
        //调用dingo接口获得tso元信息。
        this.tsoMetaService = Services.tsoService(
            Services.parse(coordinators)
        );
    }

    /**
     * 构造函数。
     * @param coordinators
     */
    public TsoService(Set<Location> coordinators) {
        //调用sdk生成元信息服务对象并赋值给本地变量。
        setTsoMetaService(Services.tsoService(coordinators));
    }

    /**
     * 设置tso元信息服务。
     * @param tsoMetaService
     */
    private void setTsoMetaService(MetaService tsoMetaService) {
        synchronized (TsoService.class) {
            this.tsoMetaService = tsoMetaService;
            if (INSTANCE.tsoMetaService == null) {
                INSTANCE.tsoMetaService = tsoMetaService;
            }
        }
    }

    /**
     * 判断当前tso服务是否可用。
     * @return
     */
    public boolean isAvailable() {
        return tsoMetaService != null;
    }

    /**
     * 用于生成tso请求的请求id。
     * @return
     */
    private long trace() {
        return Math.abs((((long) System.identityHashCode(this)) << 32) + System.nanoTime());
    }

    /**
     * 获得一个tso值。
     * @return
     */
    @Override
    public long tso() {
        TsoTimestamp startTimestamp = tsoMetaService.tsoService(
            trace(), TsoRequest.builder().opType(OP_GEN_TSO).count(1L).build()
        ).getStartTimestamp();
        return (startTimestamp.getPhysical() << PHYSICAL_SHIFT) + (startTimestamp.getLogical() & MAX_LOGICAL);
    }

    /**
     * 时间戳转tso。
     * @param timestamp
     * @return
     */
    @Override
    public long tso(long timestamp) {
        return timestamp << PHYSICAL_SHIFT;
    }

    /**
     * 获得一个时间戳。
     * @return
     */
    @Override
    public long timestamp() {
        return tsoMetaService.tsoService(
            trace(), TsoRequest.builder().opType(OP_GEN_TSO).count(1L).build()
        ).getStartTimestamp().getPhysical();
    }

    /**
     * tso转时间戳。
     * @param tso
     * @return
     */
    @Override
    public long timestamp(long tso) {
        return tso >> PHYSICAL_SHIFT;
    }
}
