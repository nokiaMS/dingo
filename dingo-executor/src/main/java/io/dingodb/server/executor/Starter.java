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

package io.dingodb.server.executor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.calcite.operation.ShowLocksOperation;
import io.dingodb.common.CommonId;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.mysql.client.SessionVariableWatched;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.driver.mysql.SessionVariableChangeWatcher;
import io.dingodb.exec.Services;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.net.MysqlNetService;
import io.dingodb.net.MysqlNetServiceProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.scheduler.SchedulerService;
import io.dingodb.server.executor.ddl.DdlContext;
import io.dingodb.server.executor.ddl.DdlServer;
import io.dingodb.server.executor.schedule.SafePointUpdateTask;
import io.dingodb.server.executor.service.ClusterService;
import io.dingodb.store.proxy.service.AutoIncrementService;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

import static io.dingodb.common.CommonId.CommonType.EXECUTOR;

@Slf4j
public class Starter {

    /**
     * 类启动参数help。
     * names：参数名字 。
     * description：描述。
     * help：参数对应变量值的默认值，此处对应的变量为help，值为true。
     * order：参数的展示顺序。
     * java的注解机制@Parameter可以给Starter类设置参数。
     */
    @Parameter(names = "--help", description = "Print usage.", help = true, order = 0)
    private boolean help;

    /**
     * Starter类的config参数。
     */
    @Parameter(names = "--config", description = "Config file path.", order = 1, required = true)
    private String config;

    /**
     * Starter类的tenant参数。
     */
    @Parameter(names = "--tenant", description = "Tenant id.", order = 2)
    private Long tenant;

    /**
     * 程序的main函数。
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //创建Starter类。
        Starter starter = new Starter();
        //构建JCommander对象。(JCommander类用于启动参数解析。)
        JCommander commander = JCommander.newBuilder().addObject(starter).build();
        //使用创建的JCommander对象解析命令行参数。
        commander.parse(args);
        //启动starter对象。
        starter.exec(commander);
    }

    /**
     * Starter类的启动函数。
     * @param commander     JCommander对象用于命令行参数解析。
     * @throws Exception    定义了类抛出的异常类型。
     */
    public void exec(JCommander commander) throws Exception {
        //如果设置了help参数，那么打印Starter类的帮助信息。
        if (help) {
            commander.usage();
            return;
        }

        //解析--confg参数指定的配置文件，当前名字为 executor.yaml，是yaml格式的文件。
        DingoConfiguration.parse(config);

        //解析tenant参数设置租户id，如果没有设置参数，那么看是否设置了环境变量tenant，如果也没有设置tenant参数，也没有设置tenant环境变量，那么默认租户id为0.
        if (tenant == null) {
            String tenantStr = System.getenv().get("tenant");
            if (tenantStr != null) {
                tenant = Long.parseLong(tenantStr);
            } else {
                tenant = 0L;
            }
        }
        //设置租户id常量值。
        TenantConstant.tenant(tenant);

        //获得server id。
        CommonId serverId = ClusterService.DEFAULT_INSTANCE.getServerId(DingoConfiguration.location());
        if (serverId == null) {
            serverId = new CommonId(EXECUTOR, 1, TsoService.getDefault().tso());
        }
        DingoConfiguration.instance().setServerId(serverId);

        //初始化配置对象实例。
        Configuration.instance();

        //开始监听端口号，此处为8765端口，为jdbc端口号。
        NetService.getDefault().listenPort(DingoConfiguration.host(), DingoConfiguration.port());
        DriverProxyServer driverProxyServer = new DriverProxyServer();
        driverProxyServer.start();
        // Register cluster heartbeat.
        ClusterService.DEFAULT_INSTANCE.register();
        // Register cluster heartbeat.
        log.info("Executor Configuration:{}", DingoConfiguration.instance());
        Services.initControlMsgService();
        Services.initNetService();

        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        env.setRole(DingoRole.EXECUTOR);

        //创建定时器服务。
        SchedulerService schedulerService = SchedulerService.getDefault();

        checkContinue();
        Object tenantObj = Optional.mapOrGet(InfoSchemaService.root(), __ -> __.getTenant(tenant), () -> null);
        if (tenantObj == null) {
            log.error("The tenant: {} was not found.", tenant);
            System.exit(0);
        }
        if (((Tenant) tenantObj).isDelete()) {
            log.error("The tenant: {} has been deleted and is unavailable", tenant);
            System.exit(0);
        }

        //定时器服务初始化。
        schedulerService.init();

        //获得mysql网络服务对象，并开始监听mysql端口，此处端口号为3307.
        MysqlNetService mysqlNetService = ServiceLoader.load(MysqlNetServiceProvider.class).iterator().next().get();
        //启动mysql兼容的sql服务，端口3307.
        mysqlNetService.listenPort(Configuration.mysqlPort());

        SessionVariableWatched.getInstance().addObserver(new SessionVariableChangeWatcher());

        // Initialize auto increment
        AutoIncrementService.INSTANCE.resetAutoIncrement();

        //注册ShowLocksOperation API接口。
        ApiRegistry.getDefault().register(ShowLocksOperation.Api.class, new ShowLocksOperation.Api() { });

        SafePointUpdateTask.run();

        //开始ddl的消息监控与分发。
        DdlServer.startDispatchLoop();
    }

    public static void checkContinue() {
        boolean ready = false;
        while (!ready) {
            if (DdlContext.getPrepare()) {
                ready = true;
            }
            Utils.sleep(500);
        }
    }

}
