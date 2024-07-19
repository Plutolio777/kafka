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

import java.util.HashSet;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 我们与集群中每个节点的连接状态。
 */
final class ClusterConnectionStates {
    final static int RECONNECT_BACKOFF_EXP_BASE = 2;
    final static double RECONNECT_BACKOFF_JITTER = 0.2;
    final static int CONNECTION_SETUP_TIMEOUT_EXP_BASE = 2;
    final static double CONNECTION_SETUP_TIMEOUT_JITTER = 0.2;
    // mark 连接状态缓存
    private final Map<String, NodeConnectionState> nodeState;
    private final Logger log;
    // mark 域名解析器
    private final HostResolver hostResolver;
    // mark 用来保存正在连接的节点ID
    private Set<String> connectingNodes;
    // mark 连接失败后等待重连的时间
    private ExponentialBackoff reconnectBackoff;
    // mark 建立连接的超时时间
    private ExponentialBackoff connectionSetupTimeout;

    public ClusterConnectionStates(long reconnectBackoffMs, long reconnectBackoffMaxMs,
                                   long connectionSetupTimeoutMs, long connectionSetupTimeoutMaxMs,
                                   LogContext logContext, HostResolver hostResolver) {
        this.log = logContext.logger(ClusterConnectionStates.class);
        // mark 这个两个玩意是相当于控制重连或者超时的时间范围 如果重连按照一个频率进行的话会有问题
        this.reconnectBackoff = new ExponentialBackoff(
                reconnectBackoffMs,
                RECONNECT_BACKOFF_EXP_BASE,
                reconnectBackoffMaxMs,
                RECONNECT_BACKOFF_JITTER);
        this.connectionSetupTimeout = new ExponentialBackoff(
                connectionSetupTimeoutMs,
                CONNECTION_SETUP_TIMEOUT_EXP_BASE,
                connectionSetupTimeoutMaxMs,
                CONNECTION_SETUP_TIMEOUT_JITTER);

        this.nodeState = new HashMap<>();
        this.connectingNodes = new HashSet<>();
        this.hostResolver = hostResolver;
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param id the connection id to check
     * @param now the current time in ms
     * @return true if we can initiate a new connection
     */
    public boolean canConnect(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            // mark 如果没有保存这个节点的连接状态则直接返回true
            return true;
        else
            // mark 如果节点时区连接 并且上次连接时间间隔已经大于重连延时则可以重新连接
            return state.state.isDisconnected() &&
                   now - state.lastConnectAttemptMs >= state.reconnectBackoffMs;
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public boolean isBlackedOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        return state != null
                && state.state.isDisconnected()
                && now - state.lastConnectAttemptMs < state.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting, return a delay based on the connection timeout.
     * When connected, wait indefinitely (i.e. until a wakeup).
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null) return 0;

        if (state.state == ConnectionState.CONNECTING) {
            return connectionSetupTimeoutMs(id);
        } else if (state.state.isDisconnected()) {
            long timeWaited = now - state.lastConnectAttemptMs;
            return Math.max(state.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Return true if a specific connection establishment is currently underway
     * @param id The id of the node to check
     */
    public boolean isConnecting(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Check whether a connection is either being established or awaiting API version information.
     * @param id The id of the node to check
     * @return true if the node is either connecting or has connected and is awaiting API versions, false otherwise
     */
    public boolean isPreparingConnection(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null &&
                (state.state == ConnectionState.CONNECTING || state.state == ConnectionState.CHECKING_API_VERSIONS);
    }

    /**
     * 将给定的节点标记为正在连接中，如有必要，移动到新的解析地址。
     * @param id 连接的id
     * @param now 当前时间（以毫秒为单位）
     * @param host 连接的主机，如果需要则在内部解析
     */
    public void connecting(String id, long now, String host) {
        // mark 尝试从节点状态缓存中获取连接状态
        NodeConnectionState connectionState = nodeState.get(id);
        // mark 如果链接状态的域名和已存在状态域名相同 则直接更新连接状态信息
        if (connectionState != null && connectionState.host().equals(host)) {
            connectionState.lastConnectAttemptMs = now;
            // mark 标记为正在连接中
            connectionState.state = ConnectionState.CONNECTING;
            // mark 移动到下一个解析的地址，或者如果地址耗尽，则标记要重新解析的节点
            connectionState.moveToNextAddress();
            // mark 添加到连接中节点集合中（只添加了个ID）
            connectingNodes.add(id);
            return;
        } else if (connectionState != null) {
            log.info("Hostname for node {} changed from {} to {}.", id, connectionState.host(), host);
        }

        // mark 如果 nodeState 尚不包含，则创建一个新的 NodeConnectionState
        // mark 添加节点状态到注册表中
        nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now,
                reconnectBackoff.backoff(0), connectionSetupTimeout.backoff(0), host, hostResolver));
        connectingNodes.add(id);
    }

    /**
     * Returns a resolved address for the given connection, resolving it if necessary.
     * @param id the id of the connection
     * @throws UnknownHostException if the address was not resolvable
     */
    public InetAddress currentAddress(String id) throws UnknownHostException {
        return nodeState(id).currentAddress();
    }

    /**
     * 使指定节点进入断开连接状态。
     * @param id 断开连接的连接ID
     * @param now 当前时间（以毫秒为单位）
     */
    public void disconnected(String id, long now) {
        // mark 先从缓存中尝试获取现有的连接状态
        NodeConnectionState nodeState = nodeState(id);
        // mark 记录一下节点的的上一次尝试连接时间
        nodeState.lastConnectAttemptMs = now;
        // mark 更新节点的退避重连时间
        updateReconnectBackoff(nodeState);
        // mark 如果节点状态当前的状态为连接中(连接中失败 需要重算重试时间)
        if (nodeState.state == ConnectionState.CONNECTING) {
            // mark 重新计算时间间隔
            updateConnectionSetupTimeout(nodeState);
            // mark 从正在连接节点列表中移除
            connectingNodes.remove(id);
        } else {
            // mark 重置重连时间
            resetConnectionSetupTimeout(nodeState);
            // mark 如果是已经建立好连接的节点 还需要清空地址
            if (nodeState.state.isConnected()) {
                // mark 如果之前已建立连接，则清除地址以触发新的 DNS 解析
                // mark 因为节点IP可能已经改变
                nodeState.clearAddresses();
            }
        }
        // mark 连接状态标记为连接失败
        nodeState.state = ConnectionState.DISCONNECTED;
    }

    /**
     * Indicate that the connection is throttled until the specified deadline.
     * @param id the connection to be throttled
     * @param throttleUntilTimeMs the throttle deadline in milliseconds
     */
    public void throttle(String id, long throttleUntilTimeMs) {
        NodeConnectionState state = nodeState.get(id);
        // The throttle deadline should never regress.
        if (state != null && state.throttleUntilTimeMs < throttleUntilTimeMs) {
            state.throttleUntilTimeMs = throttleUntilTimeMs;
        }
    }

    /**
     * Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long throttleDelayMs(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state != null && state.throttleUntilTimeMs > now) {
            return state.throttleUntilTimeMs - now;
        } else {
            return 0;
        }
    }

    /**
     * Return the number of milliseconds to wait, based on the connection state and the throttle time, before
     * attempting to send data. If the connection has been established but being throttled, return throttle delay.
     * Otherwise, return connection delay.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long pollDelayMs(String id, long now) {
        long throttleDelayMs = throttleDelayMs(id, now);
        if (isConnected(id) && throttleDelayMs > 0) {
            return throttleDelayMs;
        } else {
            return connectionDelay(id, now);
        }
    }

    /**
     * Enter the checking_api_versions state for the given node.
     * @param id the connection identifier
     */
    public void checkingApiVersions(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CHECKING_API_VERSIONS;
        resetConnectionSetupTimeout(nodeState);
        connectingNodes.remove(id);
    }

    /**
     * Enter the ready state for the given node.
     * @param id the connection identifier
     */
    public void ready(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.READY;
        nodeState.authenticationException = null;
        resetReconnectBackoff(nodeState);
        resetConnectionSetupTimeout(nodeState);
        connectingNodes.remove(id);
    }

    /**
     * Enter the authentication failed state for the given node.
     * @param id the connection identifier
     * @param now the current time in ms
     * @param exception the authentication exception
     */
    public void authenticationFailed(String id, long now, AuthenticationException exception) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.authenticationException = exception;
        nodeState.state = ConnectionState.AUTHENTICATION_FAILED;
        nodeState.lastConnectAttemptMs = now;
        updateReconnectBackoff(nodeState);
    }

    /**
     * 如果连接处于 READY 状态且当前未受到限制，则返回 true。
     *
     * @param id 连接标识符
     * @param now 当前时间（以毫秒为单位）
     */
    public boolean isReady(String id, long now) {
        return isReady(nodeState.get(id), now);
    }

    private boolean isReady(NodeConnectionState state, long now) {
        return state != null && state.state == ConnectionState.READY && state.throttleUntilTimeMs <= now;
    }

    /**
     * Return true if there is at least one node with connection in the READY state and not throttled. Returns false
     * otherwise.
     *
     * @param now the current time in ms
     */
    public boolean hasReadyNodes(long now) {
        for (Map.Entry<String, NodeConnectionState> entry : nodeState.entrySet()) {
            if (isReady(entry.getValue(), now)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if the connection has been established
     * @param id The id of the node to check
     */
    public boolean isConnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isConnected();
    }

    /**
     * Return true if the connection has been disconnected
     * @param id The id of the node to check
     */
    public boolean isDisconnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isDisconnected();
    }

    /**
     * Return authentication exception if an authentication error occurred
     * @param id The id of the node to check
     */
    public AuthenticationException authenticationException(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null ? state.authenticationException : null;
    }

    /**
     * Resets the failure count for a node and sets the reconnect backoff to the base
     * value configured via reconnect.backoff.ms
     *
     * @param nodeState The node state object to update
     */
    private void resetReconnectBackoff(NodeConnectionState nodeState) {
        nodeState.failedAttempts = 0;
        nodeState.reconnectBackoffMs = reconnectBackoff.backoff(0);
    }

    /**
     * Resets the failure count for a node and sets the connection setup timeout to the base
     * value configured via socket.connection.setup.timeout.ms
     *
     * @param nodeState The node state object to update
     */
    private void resetConnectionSetupTimeout(NodeConnectionState nodeState) {
        nodeState.failedConnectAttempts = 0;
        nodeState.connectionSetupTimeoutMs = connectionSetupTimeout.backoff(0);
    }

    /**
     * 增加失败计数器，按指数方式更新节点重连的退避时间，并记录当前时间戳。
     * 延迟时间为 reconnect.backoff.ms * 2**(failures - 1) * (+/- 20% 随机抖动)
     * 最大延迟时间（抖动前）为 reconnect.backoff.max.ms
     *
     * @param nodeState 要更新的节点状态对象
     */
    private void updateReconnectBackoff(NodeConnectionState nodeState) {
        nodeState.reconnectBackoffMs = reconnectBackoff.backoff(nodeState.failedAttempts);
        nodeState.failedAttempts++;
    }

    /**
     * Increment the failure counter and update the node connection setup timeout exponentially.
     * The delay is socket.connection.setup.timeout.ms * 2**(failures) * (+/- 20% random jitter)
     * Up to a (pre-jitter) maximum of reconnect.backoff.max.ms
     *
     * @param nodeState The node state object to update
     */
    private void updateConnectionSetupTimeout(NodeConnectionState nodeState) {
        // mark 更新建立连接失败次数
        nodeState.failedConnectAttempts++;
        // mark 重新生成节点连接超时时间（指数退避算法生成重连超时时间间隔）
        nodeState.connectionSetupTimeoutMs = connectionSetupTimeout.backoff(nodeState.failedConnectAttempts);
    }

    /**
     * Remove the given node from the tracked connection states. The main difference between this and `disconnected`
     * is the impact on `connectionDelay`: it will be 0 after this call whereas `reconnectBackoffMs` will be taken
     * into account after `disconnected` is called.
     *
     * @param id the connection to remove
     */
    public void remove(String id) {
        nodeState.remove(id);
        connectingNodes.remove(id);
    }

    /**
     * Get the state of a given connection.
     * @param id the id of the connection
     * @return the state of our connection
     */
    public ConnectionState connectionState(String id) {
        return nodeState(id).state;
    }

    /**
     * Get the state of a given node.
     * @param id the connection to fetch the state for
     */
    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }

    /**
     * Get the id set of nodes which are in CONNECTING state
     */
    // package private for testing only
    Set<String> connectingNodes() {
        return this.connectingNodes;
    }

    /**
     * Get the timestamp of the latest connection attempt of a given node
     * @param id the connection to fetch the state for
     */
    public long lastConnectAttemptMs(String id) {
        NodeConnectionState nodeState = this.nodeState.get(id);
        return nodeState == null ? 0 : nodeState.lastConnectAttemptMs;
    }

    /**
     * Get the current socket connection setup timeout of the given node.
     * The base value is defined via socket.connection.setup.timeout.
     * @param id the connection to fetch the state for
     */
    public long connectionSetupTimeoutMs(String id) {
        NodeConnectionState nodeState = this.nodeState(id);
        return nodeState.connectionSetupTimeoutMs;
    }

    /**
     * Test if the connection to the given node has reached its timeout
     * @param id the connection to fetch the state for
     * @param now the current time in ms
     */
    public boolean isConnectionSetupTimeout(String id, long now) {
        NodeConnectionState nodeState = this.nodeState(id);
        if (nodeState.state != ConnectionState.CONNECTING)
            throw new IllegalStateException("Node " + id + " is not in connecting state");
        return now - lastConnectAttemptMs(id) > connectionSetupTimeoutMs(id);
    }

    /**
     * Return the List of nodes whose connection setup has timed out.
     * @param now the current time in ms
     */
    public List<String> nodesWithConnectionSetupTimeout(long now) {
        return connectingNodes.stream()
            .filter(id -> isConnectionSetupTimeout(id, now))
            .collect(Collectors.toList());
    }

    /**
     * 节点的连接状态抽象。
     */
    private static class NodeConnectionState {

        // mark 节点的连接诶状态
        ConnectionState state;
        // mark 记录认证失败异常
        AuthenticationException authenticationException;
        // mark 上次尝试连接时间节点
        long lastConnectAttemptMs;
        // mark 期望的连接超时时间
        long failedAttempts;
        // mark 连接失败次数
        long failedConnectAttempts;
        // mark 期望的重连补偿时间
        long reconnectBackoffMs;
        // mark 建立连接超时时间
        long connectionSetupTimeoutMs;
        // mark 如果当前时间 < throttleUntilTimeMs，则连接将受到限制。
        long throttleUntilTimeMs;
        // mark 节点的连接地址
        private List<InetAddress> addresses;
        // mark 节点地址集合索引指针
        private int addressIndex;
        // mark 节点域名
        private final String host;
        // mark 域名解析器
        private final HostResolver hostResolver;

        private NodeConnectionState(ConnectionState state, long lastConnectAttempt, long reconnectBackoffMs,
                long connectionSetupTimeoutMs, String host, HostResolver hostResolver) {
            this.state = state;
            this.addresses = Collections.emptyList();
            this.addressIndex = -1;
            this.authenticationException = null;
            this.lastConnectAttemptMs = lastConnectAttempt;
            this.failedAttempts = 0;
            this.reconnectBackoffMs = reconnectBackoffMs;
            this.connectionSetupTimeoutMs = connectionSetupTimeoutMs;
            this.throttleUntilTimeMs = 0;
            this.host = host;
            this.hostResolver = hostResolver;
        }

        public String host() {
            return host;
        }

        /**
         * Fetches the current selected IP address for this node, resolving {@link #host()} if necessary.
         * @return the selected address
         * @throws UnknownHostException if resolving {@link #host()} fails
         */
        private InetAddress currentAddress() throws UnknownHostException {
            if (addresses.isEmpty()) {
                // (Re-)initialize list
                // mark 如果地址目录为空则进行域名解析
                addresses = ClientUtils.resolve(host, hostResolver);
                addressIndex = 0;
            }

            return addresses.get(addressIndex);
        }

        /**
         * 跳转到该节点的下一个可用解析地址。如果没有其他可用地址，请标记
         * 列表将在下一次 {@link #currentAddress()} 调用时刷新。
         */
        private void moveToNextAddress() {
            if (addresses.isEmpty())
                return; // Avoid div0. List will initialize on next currentAddress() call

            addressIndex = (addressIndex + 1) % addresses.size();
            if (addressIndex == 0)
                addresses = Collections.emptyList(); // Exhausted list. Re-resolve on next currentAddress() call
        }

        /**
         * Clears the resolved addresses in order to trigger re-resolving on the next {@link #currentAddress()} call.
         */
        private void clearAddresses() {
            addresses = Collections.emptyList();
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ", " + failedAttempts + ", " + throttleUntilTimeMs + ")";
        }
    }
}
