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

import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.net.netty.NettyHandlers;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.StandardSocketOptions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * mysql netty server网络服务。
 */
@Slf4j
@Getter
@Builder
public class MysqlNettyServer {
    /**
     * executor所在的主机地址。
     */
    public final String host;

    /**
     * executor监听的mysql端口号。
     */
    public final int port;

    /**
     * 连接到此端口的session列表，通常一个客户端连接对应一个connection对象。
     */
    public static final Map<String, MysqlConnection> connections = new ConcurrentHashMap<>();

    private EventLoopGroup eventLoopGroup;
    private ServerBootstrap server;

    /**
     * 启动mysql服务以进行sql处理。
     * @throws Exception
     */
    public void start() throws Exception {
        //创建并初始化server对象。
        server = new ServerBootstrap();
        eventLoopGroup = new NioEventLoopGroup(151,
            new ThreadPoolBuilder().name("mysql server " + port).coreThreads(151).maximumThreads(151).build());
        server
            .channel(NioServerSocketChannel.class)
            .group(eventLoopGroup)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
            .childOption(NioChannelOption.of(StandardSocketOptions.SO_KEEPALIVE), Boolean.TRUE)
            .childHandler(channelInitializer());

        //设置服务的主机地址及端口号。
        if (host != null) {
            server.localAddress(host, port);
        } else {
            server.localAddress(port);
        }

        //开始端口监听。
        try {
            server.bind().sync().await();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    /**
     * channel初始化函数。每个客户端连接到来会分配一个channel对象。此函数为channel的初始化函数。在channel创建之后会使用此函数对channel进行初始化。
     * @return
     */
    private ChannelInitializer<SocketChannel> channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            /**
             * channel初始化函数。
             * @param ch
             */
            @Override
            protected void initChannel(SocketChannel ch) {
                //构建一个新的MysqlConnection连接对象。
                MysqlConnection mysqlConnection = new MysqlConnection(ch);
                ch.closeFuture().addListener(f -> {
                    if (mysqlConnection.getId() != null) {
                        connections.remove(mysqlConnection.getId());
                    }
                }).addListener(f -> mysqlConnection.close());

                //设置handshake响应函数。
                ch.pipeline().addLast("handshake", new HandshakeHandler(mysqlConnection));

                //设置协议解码函数。
                ch.pipeline().addLast("decoder", new MysqlDecoder());

                //空闲状态处理函数。
                MysqlIdleStateHandler mysqlIdleStateHandler = new MysqlIdleStateHandler(
                    28800, 60);
                mysqlConnection.mysqlIdleStateHandler = mysqlIdleStateHandler;
                ch.pipeline().addLast("idleStateHandler", mysqlIdleStateHandler);

                //设置mysql消息响应处理函数。
                ch.pipeline()
                    .addLast("mysqlHandler", new MysqlHandler(mysqlConnection));

                //设置异常响应函数。
                ch.pipeline().addLast("exception", new NettyHandlers.ExceptionHandler());
            }
        };
    }

    /**
     * 关闭mysql服务。
     */
    public void close() {
        eventLoopGroup.shutdownGracefully();
    }
}
