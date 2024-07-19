/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 用于异步、多通道网络 I/O 的接口
 */
public interface Selectable {

    /**
     * See {@link #connect(String, InetSocketAddress, int, int) connect()}
     */
    int USE_DEFAULT_BUFFER_SIZE = -1;

    /**
     * 开始建立到指定地址的套接字连接。
     * @param id 这个连接的ID
     * @param address 要连接的地址
     * @param sendBufferSize 套接字的发送缓冲区大小
     * @param receiveBufferSize 套接字的接收缓冲区大小
     * @throws IOException 如果无法开始连接
     */
    void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException;

    /**
     * 如果该选择器在 I/O 上被阻止，则唤醒该选择器
     */
    void wakeup();

    /**
     * 关闭此选择器
     */
    void close();

    /**
     * 关闭由给定 id 标识的连接
     */
    void close(String id);

    /**
     * 将给定的请求排队，以便在随后的 {@link #poll(long) poll()} 调用中发送
     * @param send 要发送的请求
     */
    void send(NetworkSend send);

    /**
     * Do I/O. Reads, writes, connection establishment, etc.
     * @param timeout The amount of time to block if there is nothing to do
     * @throws IOException
     */
    void poll(long timeout) throws IOException;

    /**
     * The list of sends that completed on the last {@link #poll(long) poll()} call.
     */
    List<NetworkSend> completedSends();

    /**
     * The collection of receives that completed on the last {@link #poll(long) poll()} call.
     */
    Collection<NetworkReceive> completedReceives();

    /**
     * The connections that finished disconnecting on the last {@link #poll(long) poll()}
     * call. Channel state indicates the local channel state at the time of disconnection.
     */
    Map<String, ChannelState> disconnected();

    /**
     * The list of connections that completed their connection on the last {@link #poll(long) poll()}
     * call.
     */
    List<String> connected();

    /**
     * Disable reads from the given connection
     * @param id The id for the connection
     */
    void mute(String id);

    /**
     * Re-enable reads from the given connection
     * @param id The id for the connection
     */
    void unmute(String id);

    /**
     * Disable reads from all connections
     */
    void muteAll();

    /**
     * Re-enable reads from all connections
     */
    void unmuteAll();

    /**
     * returns true  if a channel is ready
     * @param id The id for the connection
     */
    boolean isChannelReady(String id);
}
