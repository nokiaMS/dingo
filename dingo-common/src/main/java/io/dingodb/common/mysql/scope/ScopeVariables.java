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

package io.dingodb.common.mysql.scope;

import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.util.Utils;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ScopeVariables包括了一些执行器控制开关及读写方式。
 */
public final class ScopeVariables {

    /**
     * 执行器属性集合。
     */
    private static Properties executorProp = new Properties();

    /**
     * 属性验证器，只用于验证属性是否存在。
     */
    private static Properties globalVariablesValidator = new Properties();

    /**
     * 不可变变量。
     */
    public static final List<String> immutableVariables = new ArrayList<>();

    /**
     * 字符集集合。
     */
    public static final List<String> characterSet = new ArrayList<>();

    /**
     * 初始化不可变变量集合及字符集集合。
     */
    static {
        /**
         * 不可变变量集合。
         */
        immutableVariables.add("version_comment");
        immutableVariables.add("version");
        immutableVariables.add("version_compile_os");
        immutableVariables.add("version_compile_machine");
        immutableVariables.add("license");
        immutableVariables.add("default_storage_engine");
        immutableVariables.add("have_openssl");
        immutableVariables.add("have_ssl");
        immutableVariables.add("have_statement_timeout");
        immutableVariables.add("last_insert_id");
        immutableVariables.add("@begin_transaction");

        /**
         * 字符集集合。
         */
        characterSet.add("utf8mb4");
        characterSet.add("utf8");
        characterSet.add("utf-8");
        characterSet.add("gbk");
        characterSet.add("latin1");
    }

    /**
     * 默认构造函数。
     */
    private ScopeVariables() {
    }

    /**
     * 设置所有属性。
     * @param globalVariableMap     待设置的属性值kv对。
     * @return
     */
    public static synchronized Properties putAllGlobalVar(Map<String, String> globalVariableMap) {
        //判断是否开启测量。
        if (globalVariableMap.containsKey("metric_log_enable")) {
            String metricLogEnable = globalVariableMap.get("metric_log_enable");
            metricReporter(metricLogEnable);
        }
        Properties globalVariables = new Properties();
        globalVariables.putAll(globalVariableMap);
        globalVariablesValidator = globalVariables;
        return globalVariables;
    }

    /**
     * 设置是否开启测量报告，设置为on表示开启；设置为off表示关闭。
     * @param metricLogEnable
     */
    public static synchronized void metricReporter(String metricLogEnable) {
        if ("on".equalsIgnoreCase(metricLogEnable)) {
            //开启测量报告。
            DingoMetrics.startReporter();
        } else if ("off".equalsIgnoreCase(metricLogEnable)) {
            //关闭测量报告。
            DingoMetrics.stopReporter();
        }
    }

    /**
     * 检测属性中是否包含某个key。
     * @param key
     * @return
     */
    public static synchronized boolean containsGlobalVarKey(String key) {
        return globalVariablesValidator.containsKey(key);
    }

    /**
     * 获得执行器属性rpc_batch_size，如果没有则设置为40960.
     * @return
     */
    public static Integer getRpcBatchSize() {
        return (Integer) executorProp.getOrDefault("rpc_batch_size", 40960);
    }

    /**
     * 获得属性 stats_default_size
     * @return
     */
    public static Double getStatsDefaultSize() {
        return (Double) executorProp.getOrDefault("stats_default_size", 100D);
    }

    /**
     * 获得属性 request_factor
     * @return
     */
    public static Double getRequestFactor() {
        return (Double) executorProp.getOrDefault("request_factor", 15000D);
    }

    public static boolean runDdl() {
        String runDdl = executorProp.getOrDefault("run_ddl", "on").toString();
        return runDdl.equalsIgnoreCase("on");
    }

    /**
     * enable txnScan via stream or not.  事务中全表扫描是否使用stream方式。
     * @return
     */
    public static boolean txnScanByStream() {
        String txnScanByStream = executorProp.getOrDefault("transaction_stream_scan", "on").toString();
        return txnScanByStream.equalsIgnoreCase("on");
    }

    /**
     * 获得ddl超时时间。
     * @return
     */
    public static long getDdlWaitTimeout() {
        try {
            String timeoutStr = executorProp.getOrDefault("ddl_timeout", "180000").toString();
            return Long.parseLong(timeoutStr);
        } catch (Exception e) {
            return 180000;
        }
    }

    /**
     * 事务是否启用1pc的标志位,on表示启用1pc，off表示关闭1pc。
     * @return
     */
    public static boolean transaction1Pc() {
        String transaction1Pc = executorProp.getOrDefault("transaction_1pc", "on").toString();
        return transaction1Pc.equalsIgnoreCase("on");
    }

    /**
     * 获得job2table属性。
     * @return
     */
    public static boolean getJob2Table() {
        try {
            String job2Table = executorProp.getOrDefault("job2table", "off").toString();
            return job2Table.equals("on");
        } catch (Exception e) {
            return false;
        }
    }

    public static void testIndexBlock() {
        while (true) {
            String testRun = executorProp.getOrDefault("test_index", "off").toString();
            if (testRun.equalsIgnoreCase("off")) {
                break;
            } else {
                Utils.sleep(1000);
                testRun = executorProp.getOrDefault("test_continue", "off").toString();
                if (testRun.equalsIgnoreCase("on")) {
                    executorProp.setProperty("test_continue", "off");
                    break;
                }
            }
        }
    }

    /**
     * 执行器属性设置函数。
     * @param key   待设置的key。
     * @param val   待设置的value。
     */
    public static synchronized void setExecutorProp(String key, String val) {
        if ("rpc_batch_size".equalsIgnoreCase(key)) {
            int rpcBatchSize = Integer.parseInt(val);
            executorProp.put(key, rpcBatchSize);
            return;
        } else if ("stats_default_size".equalsIgnoreCase(key)) {
            double statsDefaultSize = Double.parseDouble(val);
            executorProp.put(key, statsDefaultSize);
            return;
        } else if ("request_factor".equalsIgnoreCase(key)) {
            double requestFactor = Double.parseDouble(val);
            executorProp.put(key, requestFactor);
            return;
        }
        executorProp.put(key, val);
    }
}
