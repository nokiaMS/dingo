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

package io.dingodb.driver.mysql.netty;

import io.dingodb.common.log.LogUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * mysql空闲状态处理类。
 */
@Slf4j
@ChannelHandler.Sharable
public class MysqlIdleStateHandler extends ChannelDuplexHandler {
    //最小超时时间，1秒。
    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    // Not create a new ChannelFutureListener per write operation to reduce GC pressure.
    private final ChannelFutureListener writeListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            lastWriteTime = ticksInNanos();
        }
    };

    private volatile long idleTimeNanos;

    /**
     * 检测间隔时间。
     */
    private long interval;

    /**
     * 最后一次读时间。
     */
    private long lastReadTime;

    /**
     * 记录最后一次写入时间。
     */
    private long lastWriteTime;

    private final long delayTime;

    private ScheduledFuture<?> idleTimeoutFuture;

    /**
     * 空闲状态处理对象的状态。
     */
    private byte state; // 0 - none, 1 - initialized, 2 - destroyed

    /**
     * 通道是否处于正在读状态。
     */
    private boolean reading;

    /**
     * 定时调度器。
     */
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    /**
     * 定时任务执行的命令。
     */
    private Runnable command;

    /**
     * 构造函数。
     * @param allIdleTime
     * @param interval  检测间隔时间。
     */
    public MysqlIdleStateHandler(long allIdleTime, long interval) {
        //设置时间单位为秒。
        TimeUnit unit = TimeUnit.SECONDS;
        delayTime = unit.toNanos(5);
        if (allIdleTime <= 0) {
            idleTimeNanos = 0;
        } else {
            idleTimeNanos = Math.max(unit.toNanos(allIdleTime), MIN_TIMEOUT_NANOS);
        }
        this.interval = interval;
    }

    public void setIdleTimeout(long idleTimeout, TimeUnit unit) {
        if (idleTimeout <= 0) {
            idleTimeout = 28800;
        }
        long idleTimeNanosTmp = Math.max(unit.toNanos(idleTimeout), MIN_TIMEOUT_NANOS);
        if (idleTimeNanosTmp != idleTimeNanos) {
            if (idleTimeoutFuture != null) {
                idleTimeoutFuture.cancel(false);
                idleTimeoutFuture = null;
            }
            lastReadTime = lastWriteTime = ticksInNanos();
            idleTimeNanos = idleTimeNanosTmp;
            if (idleTimeNanos > 0) {
                idleTimeoutFuture = schedule(command, interval, TimeUnit.SECONDS);
                LogUtils.info(log, "modify idleTimeNanos:" + idleTimeNanos);
            }
        }
    }

    /**
     * Return the allIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getAllIdleTimeInSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(idleTimeNanos);
    }

    /**
     * 当此句柄被加入channel时执行。
     * @param ctx
     * @throws Exception
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
            initialize(ctx);
        }
    }

    /**
     * 当此句柄从通道中移除时执行。
     * @param ctx
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        destroy();
    }

    /**
     * 当注册此句柄时执行此函数。
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    /**
     * 通道变为活跃时执行此函数。
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelActive() event is fired.  If a user adds this handler
        // after the channelActive() event, initialize() will be called by beforeAdd().
        initialize(ctx);
        super.channelActive(ctx);
    }

    /**
     * 通道断开连接时执行此函数。
     * @param ctx
     * @param promise
     * @throws Exception
     */
    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.disconnect(ctx, promise);
    }

    /**
     * 通道关闭时执行此函数。
     * @param ctx
     * @param promise
     * @throws Exception
     */
    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.close(ctx, promise);
    }

    /**
     * 通道解除注册时执行此函数。
     * @param ctx
     * @param promise
     * @throws Exception
     */
    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.deregister(ctx, promise);
    }

    /**
     * 通道未注册时执行此函数。
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    /**
     * 通道变为不活跃时执行此函数。
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        //super.channelInactive(ctx);
        ctx.close();
    }

    /**
     * channel读取函数。
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (idleTimeNanos > 0) {
            //设置channel正在读取。
            reading = true;
        }
        //读取消息。
        ctx.fireChannelRead(msg);
    }

    /**
     * channel读取完毕。
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if ((idleTimeNanos > 0) && reading) {
            lastReadTime = ticksInNanos();
            reading = false;
        }
        ctx.fireChannelReadComplete();
    }

    /**
     * 向通道写入消息。
     * @param ctx
     * @param msg
     * @param promise
     * @throws Exception
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Allow writing with void promise if handler is only configured for read timeout events.
        if (idleTimeNanos > 0) {
            ctx.write(msg, promise.unvoid()).addListener(writeListener);
        } else {
            ctx.write(msg, promise);
        }
    }

    /**
     * 初始化。
     * @param ctx
     */
    private void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
            case 1:
            case 2:
                return;
            default:
                break;
        }

        state = 1;

        //设置最后一次读时间与写时间为当前时间。
        lastReadTime = lastWriteTime = ticksInNanos();
        if (idleTimeNanos > 0) {
            command = new MysqlIdleStateHandler.IdleTimeoutTask(ctx);
            idleTimeoutFuture = schedule(command,
                interval, TimeUnit.SECONDS);
            LogUtils.info(log, "init idleTimeout task: " + idleTimeNanos);
        }
    }

    /**
     * 返回当前纳秒时间。
     * This method is visible for testing!.
     */
    long ticksInNanos() {
        return System.nanoTime();
    }

    /**
     * 定时任务调度函数。
     * @param task
     * @param delay
     * @param unit
     * @return
     */
    ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        return service.scheduleAtFixedRate(task, delayTime, delay, unit);
    }

    /**
     * 销毁定时任务。
     */
    private void destroy() {
        state = 2;

        if (idleTimeoutFuture != null && !idleTimeoutFuture.isCancelled()) {
            idleTimeoutFuture.cancel(false);
            idleTimeoutFuture = null;
        }
        if (service != null && !service.isShutdown()) {
            service.shutdown();
        }
        command = null;
    }

    /**
     * 抽象空闲任务。
     */
    private abstract static class AbstractIdleTask implements Runnable {

        /**
         * channel上下文。
         */
        private final ChannelHandlerContext ctx;

        /**
         * 构造函数。
         * @param ctx
         */
        AbstractIdleTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        /**
         * 任务运行函数。
         */
        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            run(ctx);
        }

        /**
         * 任务运行函数。
         * @param ctx
         */
        protected abstract void run(ChannelHandlerContext ctx);
    }

    /**
     * 创建空闲超时定时处理任务。
     */
    private final class IdleTimeoutTask extends MysqlIdleStateHandler.AbstractIdleTask {

        /**
         * 构造函数。
         * @param ctx
         */
        IdleTimeoutTask(ChannelHandlerContext ctx) {
            super(ctx);
        }

        /**
         * task运行函数。
         * @param ctx
         */
        @Override
        protected void run(ChannelHandlerContext ctx) {

            long nextDelay = idleTimeNanos;
            if (!reading) {
                nextDelay -= ticksInNanos() - Math.max(lastReadTime, lastWriteTime);
            }
            LogUtils.info(log, "nextDelay:" + nextDelay + ", idleTimeNanos:" + idleTimeNanos);
            if (nextDelay <= 0) {
                try {
                    if (service != null && !service.isShutdown()) {
                        service.shutdown();
                    }
                    if (idleTimeoutFuture != null && !idleTimeoutFuture.isCancelled()) {
                        idleTimeoutFuture.cancel(true);
                    }
                    service = null;
                    idleTimeoutFuture = null;
                    LogUtils.info(log, "this channel will close, mysql connection current count:"
                        + MysqlNettyServer.connections.size());
                    ctx.channel().close();
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            }
        }
    }
}
