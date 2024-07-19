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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * NetworkClient 用于请求更新集群元数据信息并检索集群节点的接口
 * 来自此类元数据。这是一个内部类。
 * <p>
 * 这个类不是线程安全的！
 * 默认使用 {@link ManualMetadataUpdater}
 * 还有一个默认实现 {@link NetworkClient.DefaultMetadataUpdater}
 */
public interface MetadataUpdater extends Closeable {

    /**
     * 无阻塞获取当前集群信息。
     */
    List<Node> fetchNodes();

    /**
     * 如果集群元数据信息更新到期，则返回 true。
     */
    boolean isUpdateDue(long now);

    /**
     * 如果需要且可能，启动一个集群元数据更新。返回直到元数据更新的时间（如果启动了更新，则时间为 0）。
     * 如果实现依赖于 `NetworkClient` 来发送请求，`handleSuccessfulResponse` 方法将在接收到元数据响应后被调用。
     * `needed` 和 `possible` 的语义依赖于具体的实现，可能会考虑多种因素，如节点可用性、距离上次元数据更新的时间等。
     */
    long maybeUpdate(long now);

    /**
     * Handle a server disconnect.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for disconnections of such requests.
     *
     * @param now Current time in milliseconds
     * @param nodeId The id of the node that disconnected
     * @param maybeAuthException Optional authentication error
     */
    void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException);

    /**
     * Handle a metadata request failure.
     *
     * @param now Current time in milliseconds
     * @param maybeFatalException Optional fatal error (e.g. {@link UnsupportedVersionException})
     */
    void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException);

    /**
     * Handle responses for metadata requests.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for completed receives of such requests.
     */
    void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse);

    /**
     * Close this updater.
     */
    @Override
    void close();
}
