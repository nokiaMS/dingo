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

package io.dingodb.common.ddl;

import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.EventObject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import javax.swing.event.EventListenerList;

/**
 * ddl job事件源。
 *      此对象初始化后会启动两个线程，分别监听队列 ownerJobQueue 和 mdlCheckVerQueue，
 *      一旦这两个队列中有消息，就调用对应监听器的响应函数。
 */
@Slf4j
public final class DdlJobEventSource {
    //事件监听器列表。
    private final EventListenerList listenerList = new EventListenerList();

    //版本监听器列表。
    private final EventListenerList verListenerList = new EventListenerList();

    //返回ddl job事件源单例。
    public static DdlJobEventSource ddlJobEventSource = new DdlJobEventSource();

    //ddl job事件队列。
    public final BlockingQueue<Long> ownerJobQueue = new LinkedBlockingDeque<>(1000);

    //版本检测时间队列。
    public final BlockingQueue<Long> mdlCheckVerQueue = new LinkedBlockingDeque<>(1000);

    /**
     * 构造函数。
     */
    private DdlJobEventSource() {
        //获得当前执行环境。
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;

        //启动线程监听ownerJobQueue队列。
        new Thread(() -> {
            while (true) {
                //只有ddl owner会处理job队列。
                if (!env.ddlOwner.get()) {
                    Utils.sleep(5000);
                    continue;
                }
                try {
                    //从队列中读取事件。
                    long jobNotify = take(ownerJobQueue);
                    //通知监听器。
                    ddlJob(jobNotify);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();

        //启动线程监听mdlCheckVerQueue队列。
        new Thread(() -> {
            while (true) {
                try {
                    //读取事件。
                    long checkVerNotify = take(mdlCheckVerQueue);
                    //通知监听器。
                    ddlCheckVer(checkVerNotify);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();
    }

    /**
     * 从队列中读取一个事件。
     * @param queue
     * @return
     */
    public Long take(BlockingQueue<Long> queue) {
        while (true) {
            try {
                //从队列中读取数据。
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * 添加ddl job监听器。
     * @param listener
     */
    public void addListener(DdlJobListener listener) {
        listenerList.add(DdlJobListener.class, listener);
    }

    /**
     * 添加版本检测监听器。
     * @param listener
     */
    public void addMdlCheckVerListener(DdlCheckMdlVerListener listener) {
        verListenerList.add(DdlCheckMdlVerListener.class, listener);
    }

    /**
     * 构造ddlJob事件并通知ddl job事件监听器。
     * @param size
     */
    public void ddlJob(long size) {
        //创建ddl job事件。
        DdlJobEvent event = new DdlJobEvent(size);
        //获得监听器。
        Object[] listeners = listenerList.getListenerList();
        if (listeners == null) {
            return;
        }
        //通知监听器事件发生。
        for (Object listener : listeners) {
            if (listener instanceof DdlJobListener) {
                //for (int i = 0; i < size; i ++) {
                DdlJobListener ddlJobListener = (DdlJobListener) listener;
                ddlJobListener.eventOccurred(event);
                //}
            }
        }
    }

    /**
     * 构造版本检测事件并通知版本检测事件监听器。
     * @param size
     */
    public void ddlCheckVer(long size) {
        EventObject event = new EventObject(size);
        Object[] listeners = verListenerList.getListenerList();
        if (listeners == null) {
            return;
        }
        for (Object listener : listeners) {
            if (listener instanceof DdlCheckMdlVerListener) {
                ((DdlCheckMdlVerListener) listener).eventOccurred(event);
            }
        }
    }

    /**
     * 向队列中添加事件。
     * @param queue
     * @param item
     * @param <T>
     */
    public static <T> void forcePut(@NonNull BlockingQueue<T> queue, T item) {
        while (true) {
            try {
                queue.put(item);
                break;
            } catch (InterruptedException ignored) {
            }
        }
    }
}
