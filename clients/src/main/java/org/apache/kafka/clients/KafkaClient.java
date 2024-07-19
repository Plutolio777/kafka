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
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.Closeable;
import java.util.List;

/**
 * {@link NetworkClient} 的接口
 */
public interface KafkaClient extends Closeable {

    /**
     * 检查当前是否准备好向指定节点发送另一个请求，但如果未准备好则不尝试连接。
     *
     * @param node 要检查的节点
     * @param now 当前时间戳
     * @return 如果准备好发送请求，则返回 true；否则返回 false
     */
    boolean isReady(Node node, long now);

    /**
     * 向指定节点发起连接（如果需要），并在已连接的情况下返回 true。节点的准备状态仅在调用 poll 时才会改变。
     *
     * @param node 要连接的节点
     * @param now 当前时间
     * @return 如果立即准备好向指定节点发送另一个请求，则返回 true
     */
    boolean ready(Node node, long now);

    /**
     * 根据连接状态返回等待的毫秒数，以在尝试发送数据之前。断开连接时，会尊重重新连接的退避时间。连接或已连接时，这会处理慢/停滞的连接。
     *
     * @param node 要检查的节点
     * @param now 当前时间戳
     * @return 要等待的毫秒数
     */
    long connectionDelay(Node node, long now);

    /**
     * 根据连接状态和节流时间返回等待的毫秒数，以在尝试发送数据之前。如果连接已建立但正在被节流，则返回节流延迟。否则，返回连接延迟。
     *
     * @param node 要检查的连接
     * @param now 当前时间（以毫秒为单位）
     * @return 要等待的毫秒数
     */
    long pollDelayMs(Node node, long now);

    /**
     * 根据连接状态检查节点的连接是否失败。这种连接失败通常是短暂的，可以在下一次 {@link #ready(org.apache.kafka.common.Node, long)} 调用中恢复，但有些情况下需要捕捉和处理这些短暂的失败。
     *
     * @param node 要检查的节点
     * @return 如果连接失败且节点断开连接，则返回 true
     */
    boolean connectionFailed(Node node);

    /**
     * 根据连接状态检查此节点的身份验证是否失败。身份验证失败不会进行任何重试。
     *
     * @param node 要检查的节点
     * @return 如果身份验证失败，则返回 AuthenticationException，否则返回 null
     */
    AuthenticationException authenticationException(Node node);

    /**
     * 将给定请求排队等待发送。请求只能在准备好的连接上发送。
     *
     * @param request 请求
     * @param now 当前时间戳
     */
    void send(ClientRequest request, long now);

    /**
     * 从套接字中实际进行读写操作。
     *
     * @param timeout 响应的最大等待时间（以毫秒为单位），必须是非负数。如果合适，实施可以使用更低的值（常见原因包括较低的请求或元数据更新超时）
     * @param now 当前时间（以毫秒为单位）
     * @throws IllegalStateException 如果请求被发送到未准备好的节点
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * 断开与特定节点的连接（如果存在）。
     * 对于此连接的任何挂起的 ClientRequests 将收到断开连接的通知。
     *
     * @param nodeId 节点的 ID
     */
    void disconnect(String nodeId);

    /**
     * 关闭与特定节点的连接（如果存在）。
     * 所有请求都将被清除。对于已清除的请求，不会调用 ClientRequest 回调，也不会从 poll() 中返回。
     *
     * @param nodeId 节点的 ID
     */
    void close(String nodeId);

    /**
     * 选择拥有最少未完成请求的节点。此方法会优先选择已有连接的节点，但如果所有现有连接都在使用中，可能会选择尚未建立连接的节点。
     *
     * @param now 当前时间（以毫秒为单位）
     * @return 拥有最少未完成请求的节点
     */
    Node leastLoadedNode(long now);

    /**
     * 当前尚未返回响应的未完成请求的数量
     */
    int inFlightRequestCount();

    /**
     * 如果至少有一个未完成的请求，则返回 true；否则返回 false。
     */
    boolean hasInFlightRequests();

    /**
     * 获取特定节点的总未完成请求数量
     *
     * @param nodeId 节点的 ID
     * @return 特定节点的未完成请求数量
     */
    int inFlightRequestCount(String nodeId);

    /**
     * 如果特定节点有至少一个未完成的请求，则返回 true；否则返回 false。
     *
     * @param nodeId 节点的 ID
     * @return 如果节点有未完成请求，则返回 true
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * 如果至少有一个连接状态为 READY 且未被节流的节点，则返回 true；否则返回 false。
     *
     * @param now 当前时间
     * @return 如果存在 READY 状态且未被节流的节点，则返回 true
     */
    boolean hasReadyNodes(long now);

    /**
     * 如果客户端当前因等待 I/O 而被阻塞，则唤醒客户端
     */
    void wakeup();

    /**
     * 创建一个新的 ClientRequest。
     *
     * @param nodeId 发送目标节点的 ID
     * @param requestBuilder 用于构建请求的构建器
     * @param createdTimeMs 用作请求创建时间的时间（以毫秒为单位）
     * @param expectResponse 如果期望响应，则为 true
     * @return 新创建的 ClientRequest
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs, boolean expectResponse);

    /**
     * 创建一个新的 ClientRequest。
     *
     * @param nodeId 发送目标节点的 ID
     * @param requestBuilder 用于构建请求的构建器
     * @param createdTimeMs 用作请求创建时间的时间（以毫秒为单位）
     * @param expectResponse 如果期望响应，则为 true
     * @param requestTimeoutMs 等待响应的最大时间（以毫秒为单位），在断开套接字和取消请求之前。请求可能会在套接字因任何原因断开连接之前被取消，包括如果对同一节点的另一个挂起请求首先超时。
     * @param callback 响应到达时调用的回调
     * @return 新创建的 ClientRequest
     */
    ClientRequest newClientRequest(String nodeId,
                                   AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs,
                                   boolean expectResponse,
                                   int requestTimeoutMs,
                                   RequestCompletionHandler callback);

    /**
     * 发起客户端的关闭操作。此方法可以在客户端被轮询时从其他线程调用。无法使用客户端发送进一步的请求。当前的 poll() 将使用 wakeup() 终止。应在 poll 返回后显式关闭客户端，使用 {@link #close()}。请注意，在轮询时不应同时调用 {@link #close()}。
     */
    void initiateClose();

    /**
     * 如果客户端仍然活跃，则返回 true。如果已调用 {@link #initiateClose()} 或 {@link #close()}，则返回 false。
     */
    boolean active();

}