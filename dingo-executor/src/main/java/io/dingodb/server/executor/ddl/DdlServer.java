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

package io.dingodb.server.executor.ddl;

import com.codahale.metrics.Timer;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlJobEvent;
import io.dingodb.common.ddl.DdlJobEventSource;
import io.dingodb.common.ddl.DdlJobListenerImpl;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.sdk.service.WatchService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.server.executor.Configuration;
import io.dingodb.store.proxy.ddl.DdlHandler;
import io.dingodb.store.service.InfoSchemaService;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public final class DdlServer {
    public static BlockingQueue<Long> verDelQueue = new LinkedBlockingDeque<>(10000);
    private DdlServer() {
    }

    /**
     * 监控ddlJob队列。
     */
    public static void watchDdlJob() {
        //创建ddl job事件监听器。
        DdlJobListenerImpl ddlJobListener = new DdlJobListenerImpl(DdlServer::startLoadDDLAndRun);
        //构造ddl job事件源。
        DdlJobEventSource ddlJobEventSource = DdlJobEventSource.ddlJobEventSource;
        //添加ddl job事件的事件监听器。
        ddlJobEventSource.addListener(ddlJobListener);
        //删除保存的schema差异对象。
        if (DdlUtil.delDiff) {
            delVerSchemaDiff();
        }
    }

    public static void delVerSchemaDiff() {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        new Thread(() -> {
            while (true) {
                if (!env.ddlOwner.get()) {
                    Utils.sleep(5000);
                    continue;
                }
                try {
                    long ver = DdlJobEventSource.ddlJobEventSource.take(verDelQueue);
                    if (ver > 220) {
                        ver = ver - 110;
                        io.dingodb.meta.InfoSchemaService.root().delSchemaDiff(ver);
                        DingoMetrics.counter("delSchemaDiff").inc();
                    }
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();
    }

    /**
     * 监听coordinator上ddl job并发key ADDING_DDL_JOB_CONCURRENT_KEY的变化，发生变化则调用DdlServer::startLoadDDLAndRunByEtcd处理函数。
     */
    public static void watchDdlKey() {
        //String resourceKey = String.format("tenantId:{%d}", TenantConstant.TENANT_ID);

        //构建coordinator的监控服务对象。
        WatchService watchService = new WatchService(Configuration.coordinators());
        //LockService lockService = new LockService(resourceKey, Configuration.coordinators(), 45000);

        //构造kv对象，key为ADDING_DDL_JOB_CONCURRENT_KEY
        Kv kv = Kv.builder().kv(KeyValue.builder()
            .key(DdlUtil.ADDING_DDL_JOB_CONCURRENT_KEY.getBytes()).build()).build();
        try {
            //监听coordinator上的ADDING_DDL_JOB_CONCURRENT_KEY的变化，如果有变动，会调用处理函数DdlServer::startLoadDDLAndRunByEtcd进行处理。
            watchService.watchAllOpEvent(kv, DdlServer::startLoadDDLAndRunByEtcd);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            watchDdlKey();
        }
    }

    public static String startLoadDDLAndRunByEtcd(String typeStr) {
        if (typeStr.equals("keyNone")) {
            Utils.sleep(1000);
            return "none";
        }
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            session.setAutoCommit(true);
            startLoadDDLAndRun(session);
            return "done";
        } catch (Exception e) {
            LogUtils.error(log, "startLoadDDLAndRunByEtcd error, reason:{}", e.getMessage());
            return "runError";
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    /**
     * ddl job event的响应函数，获得event并进行处理。
     * @param ddlJobEvent
     * @return
     */
    public static boolean startLoadDDLAndRun(DdlJobEvent ddlJobEvent) {
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            LogUtils.info(log, "startJob by local event");
            session.setAutoCommit(true);
            startLoadDDLAndRun(session);
        } catch (Exception e) {
            LogUtils.error(log, "startLoadDDLAndRun by event error, reason:{}", e.getMessage());
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        return true;
    }

    /**
     * 开始ddl的消息监控与分发。
     */
    public static void startDispatchLoop() {
        // ticker/watchDdlJobEvent/watchDdlJobCoordinator
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        //只有ddl owner才能分发消息。
        while (!env.ddlOwner.get()) {
            Utils.sleep(1000);
        }
        //监控ddlJob队列。
        watchDdlJob();
        //监听ddl key。
        watchDdlKey();

        Session session = SessionUtil.INSTANCE.getSession();
        session.setAutoCommit(true);
        Executors.scheduleWithFixedDelayAsync("DdlWorker", () -> startLoadDDLAndRunBySchedule(session), 10000, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * 定时任务，每隔1秒执行一次。
     * @param session
     */
    public static void startLoadDDLAndRunBySchedule(Session session) {
        //LogUtils.info(log, "startJob by local schedule");
        //从表中查询ddl job并运行。
        startLoadDDLAndRun(session);
    }

    /**
     * 处理ddl job.
     * @param session
     */
    public static void startLoadDDLAndRun(Session session) {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        //只有owner才能够处理ddl job。
        // if owner continue,not break;
        if (!env.ddlOwner.get()
            || DdlContext.INSTANCE.waiting.get()
            || !DdlContext.INSTANCE.prepare.get()
            || !ScopeVariables.runDdl()
        ) {
            LogUtils.info(log, "not ready, owner:{}", env.ddlOwner.get());
            DdlContext.INSTANCE.getWc().setOnceVal(true);
            Utils.sleep(1000);
            return;
        }

        //加载并处理ddl job。
        loadDDLJobsAndRun(session, JobTableUtil::getGenerateJobs, DdlContext.INSTANCE.getDdlJobPool());
    }

    /**
     * 加载并处理ddl job：
     *      此函数从dingo_ddl_job表中查找job对象，并转交给ddl worker对象进行处理。
     * @param session   当前session。
     * @param getJob    job获取函数，从dingo_ddl_job中获得表ddlJob行并转换为job对象。
     * @param pool      ddl worker pool对象。
     */
    static synchronized void loadDDLJobsAndRun(Session session, Function<Session, Pair<List<DdlJob>, String>> getJob, DdlWorkerPool pool) {
        long start = System.currentTimeMillis();
        //获得dingo_ddl_job表中的jobs。
        Pair<List<DdlJob>, String> res = getJob.apply(session);
        if (res == null || res.getValue() != null) {
            return;
        }
        List<DdlJob> ddlJobs = res.getKey();
        if (ddlJobs == null || ddlJobs.isEmpty()) {
            return;
        }
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("loadDdlJobs").update(sub, TimeUnit.MILLISECONDS);
        try {
            if (ddlJobs.size() > 1) {
                LogUtils.info(log, "ddl-jobs size:" + ddlJobs.size());
            }
            for (DdlJob ddlJob : ddlJobs) {
                DdlWorker worker = pool.borrowObject();
                //调用ddl worker对象对ddl job进行处理。
                delivery2worker(worker, ddlJob, pool);
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    static synchronized void loadDDLJobAndRun(Session session, Function<Session, Pair<DdlJob, String>> getJob, DdlWorkerPool pool) {
        long start = System.currentTimeMillis();
        Pair<DdlJob, String> res = getJob.apply(session);
        if (res == null || res.getValue() != null) {
            return;
        }
        DdlJob ddlJob = res.getKey();
        if (ddlJob == null) {
            return;
        }
        long sub = System.currentTimeMillis() - start;
        if (sub > 150) {
            LogUtils.info(log, "get job cost:{}", sub);
        }
        DingoMetrics.timer("loadDdlJob").update(sub, TimeUnit.MILLISECONDS);
        try {
            DdlWorker worker = pool.borrowObject();
            delivery2worker(worker, ddlJob, pool);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public static void delivery2worker(DdlWorker worker, DdlJob ddlJob, DdlWorkerPool pool) {
        DdlContext dc = DdlContext.INSTANCE;
        dc.insertRunningDDLJobMap(ddlJob.getId());
        LogUtils.info(log, "delivery 2 worker, jobId:{}, state:{}", ddlJob.getId(), ddlJob.getState());
        //并发方式执行ddl-worker。
        Executors.submit("ddl-worker", () -> {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("ddlJobRun");
            try {
                if (!dc.getWc().isSynced(ddlJob.getId()) || dc.getWc().getOnce().get()) {
                    if (DdlUtil.mdlEnable) {
                        try {
                            Pair<Boolean, Long> res = checkMDLInfo(ddlJob.getId());
                            if (res.getKey()) {
                                pool.returnObject(worker);
                                String error = DdlWorker.waitSchemaSyncedForMDL(dc, ddlJob, res.getValue());
                                if (error != null) {
                                    LogUtils.warn(log, "[ddl] check MDL info failed, jobId:{}", ddlJob.getId());
                                    return;
                                }
                                DdlContext.INSTANCE.getWc().setOnceVal(false);
                                JobTableUtil.cleanMDLInfo(ddlJob.getId());
                                return;
                            }
                        } catch (Exception e) {
                            pool.returnObject(worker);
                            LogUtils.warn(log, "[ddl] check MDL info failed, jobId:{}", ddlJob.getId());
                            return;
                        }
                    } else {
                        try {
                            waitSchemaSynced(dc, ddlJob, 2 * dc.getLease(), worker);
                        } catch (Exception e) {
                            pool.returnObject(worker);
                            LogUtils.error(log, "[ddl] wait ddl job sync failed, reason:" + e.getMessage() + ", job:" + ddlJob);
                            Utils.sleep(1000);
                            return;
                        }
                        dc.getWc().setOnceVal(false);
                    }
                }

                //处理ddl job。
                Pair<Long, String> res = worker.handleDDLJobTable(dc, ddlJob);  //把ddl操作发送给coordinator。
                if (res.getValue() != null) {
                    LogUtils.error(log, "[ddl] handle ddl job failed, jobId:{}, error:{}", ddlJob.getId(), res.getValue());
                } else {
                    long schemaVer = res.getKey();
                    waitSchemaChanged(dc, 2 * dc.getLease(), schemaVer, ddlJob, worker);
                    JobTableUtil.cleanMDLInfo(ddlJob.getId());
                    dc.getWc().synced(ddlJob);
                }
            } catch (Exception e) {
                LogUtils.error(log, "delivery2worker failed", e);
            } finally {
                if (ddlJob.isDone() || ddlJob.isRollbackDone()) {
                    if (ddlJob.isDone()) {
                        ddlJob.setState(JobState.jobStateSynced);
                    }
                    long start = System.currentTimeMillis();
                    String error = worker.handleJobDone(ddlJob);
                    long sub = System.currentTimeMillis() - start;
                    DingoMetrics.timer("handleJobDone").update(sub, TimeUnit.MILLISECONDS);
                    if (error != null) {
                        LogUtils.error(log, "[ddl-error] handle job done error:{}", error);
                    }
                }
                dc.deleteRunningDDLJobMap(ddlJob.getId());
                pool.returnObject(worker);
                timeCtx.stop();
                LogUtils.info(log, "job loop done,jobId:{}", ddlJob.getId());
                DdlHandler.asyncNotify(1L);
            }
        });
    }

    static Pair<Boolean, Long> checkMDLInfo(long jobId) throws SQLException {
        String sql = "select version from mysql.dingo_mdl_info where job_id = " + jobId;
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            List<Object[]> objList = session.executeQuery(sql);
            if (objList.isEmpty()) {
                return Pair.of(false, 0L);
            }
            long ver = (long) objList.get(0)[0];
            return Pair.of(true, ver);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    static void waitSchemaSynced(DdlContext ddlContext, DdlJob job, long waitTime, DdlWorker worker) {
        if (!job.isRunning() && !job.isRollingback() && !job.isDone() && !job.isRollbackDone()) {
            return;
        }
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        long latestSchemaVersion = infoSchemaService.getSchemaVersionWithNonEmptyDiff();
        waitSchemaChanged(ddlContext, waitTime, latestSchemaVersion, job, worker);
    }

    public static void waitSchemaChanged(
        DdlContext dc,
        long waitTime,
        long latestSchemaVersion,
        DdlJob job,
        DdlWorker ddlWorker
    ) {
        if (!job.isRunning() && !job.isRollingback() && !job.isDone() && !job.isRollbackDone()) {
            return;
        }
        if (waitTime == 0) {
            return;
        }
        long start = System.currentTimeMillis();
        if (latestSchemaVersion == 0) {
            LogUtils.error(log, "[ddl] schema version doesn't change, jobId:{}", job.getId());
            return;
        }
        try {
            dc.getSchemaSyncer().ownerUpdateGlobalVersion(latestSchemaVersion);
            LogUtils.info(log, "owner update global ver:{}", latestSchemaVersion);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] update latest schema version failed, version:" + latestSchemaVersion, e);
        }
        try {
            String error = dc.getSchemaSyncer()
                .ownerCheckAllVersions(job.getId(), latestSchemaVersion, job.mayNeedReorg());
            if (error != null) {
                if ("Lock wait timeout exceeded".equalsIgnoreCase(error)) {
                    job.encodeError(error);
                    ddlWorker.updateDDLJob(job, false);
                }
                LogUtils.error(log, "[ddl] wait latest schema version encounter error, latest version:{}, jobId:{}" , latestSchemaVersion, job.getId());
                return;
            } else {
                if (DdlUtil.delDiff) {
                    verDelQueue.put(latestSchemaVersion);
                }
                long sub = System.currentTimeMillis() - start;
                DingoMetrics.timer("mdlWaitChanged").update(sub, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] wait latest schema version encounter error, latest version:" + latestSchemaVersion, e);
            return;
        }
        long end = System.currentTimeMillis();
        LogUtils.info(log, "[ddl] wait latest schema version changed,version: {}, take time:{}, jobId:{}", latestSchemaVersion, (end - start), job.getId());
    }

}
