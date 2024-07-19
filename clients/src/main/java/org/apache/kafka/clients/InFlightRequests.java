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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这里定义了一个用于收集已发送或正在发送但尚未收到响应的请求集合
 */
final class InFlightRequests {

    // mark 这个配置规定了已发送或正在发送但尚未收到响应的请求集合数量
    private final int maxInFlightRequestsPerConnection;
    // mark 存储请求示例（key为目的地IP地址 value为双端队列）
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    /** Thread safe total number of in flight requests. */
    // mark 使用原子类计算当前的请求数量
    private final AtomicInteger inFlightRequestCount = new AtomicInteger(0);

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * 将给定的请求添加到InFlight
     *
     * @param request 要添加的请求对象
     */
    public void add(NetworkClient.InFlightRequest request) {
        // mark 获取连接的目标地址
        String destination = request.destination;
        // mark 根据地址获取双端队列
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.get(destination);
        //noinspection Java8MapApi
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(destination, reqs);
        }
        // mark 添加到双端队列头部
        reqs.addFirst(request);
        // mark 计数+1
        inFlightRequestCount.incrementAndGet();
    }

    /**
     * 获取给定节点的请求队列。
     *
     * @param node 节点的标识符
     * @return 给定节点的请求队列
     * @throws IllegalStateException 如果节点的请求队列为空
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * 获取给定节点的最早请求（即将完成的下一个请求）。
     *
     * @param node 节点的标识符
     * @return 给定节点的最早请求对象
     */
    public NetworkClient.InFlightRequest completeNext(String node) {
        // mark 从队尾获取request
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollLast();
        // mark 计数-1
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * 获取我们发送到给定节点的最后一个请求（但不从队列中移除它）。
     *
     * @param node 节点的标识符
     * @return 最后一个发送到给定节点的请求对象
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        // mark 获取队首的request
        return requestQueue(node).peekFirst();
    }

    /**
     * 完成发送到特定节点的最后一个请求。
     *
     * @param node 请求发送的节点
     * @return 完成的请求对象
     */
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollFirst();
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * 我们是否可以向此节点发送更多请求？
     *
     * @param node 相关节点
     * @return 如果我们没有仍在发送到指定节点的请求，则返回true
     */
    public boolean canSendMore(String node) {
        // mark 根据节点获取队列
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        // mark 判断条件
        // mark 1. 队列为空
        // mark 2. 队列不为空，且队首的request已经完成，并且队列的大小小于最大允许的请求数量
        return queue == null || queue.isEmpty() ||
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int count(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    public boolean isEmpty(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
     */
    public int count() {
        return inFlightRequestCount.get();
    }

    /**
     * Return true if there is no in-flight request and false otherwise
     */
    public boolean isEmpty() {
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values()) {
            if (!deque.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * 清除给定节点的所有正在进行中的请求并返回它们。
     *
     * @param node 节点
     * @return 已移除的该节点的所有正在进行中的请求
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            final Deque<NetworkClient.InFlightRequest> clearedRequests = requests.remove(node);
            inFlightRequestCount.getAndAdd(-clearedRequests.size());
            //noinspection Convert2MethodRef
            return () -> clearedRequests.descendingIterator();
        }
    }

    private Boolean hasExpiredRequest(long now, Deque<NetworkClient.InFlightRequest> deque) {
        for (NetworkClient.InFlightRequest request : deque) {
            if (request.timeElapsedSinceSendMs(now) > request.requestTimeoutMs)
                return true;
        }
        return false;
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @return list of nodes
     */
    public List<String> nodesWithTimedOutRequests(long now) {
        List<String> nodeIds = new ArrayList<>();
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> requestEntry : requests.entrySet()) {
            String nodeId = requestEntry.getKey();
            Deque<NetworkClient.InFlightRequest> deque = requestEntry.getValue();
            if (hasExpiredRequest(now, deque))
                nodeIds.add(nodeId);
        }
        return nodeIds;
    }

}
