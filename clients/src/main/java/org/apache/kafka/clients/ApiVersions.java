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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 该类用于在 Kafka 的 NetworkClient 外部访问节点 API 版本信息。
 * 这种模式类似于使用 Metadata 类获取主题元数据。
 *
 * Maintains node api versions for access outside of NetworkClient (which is where the information is derived).
 * The pattern is akin to the use of {@link Metadata} for topic metadata.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public class ApiVersions {

    // 存储每个节点对应的 API 版本信息。
    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();

    // 存储可用的最大 Produce 协议版本号。 v2
    private byte maxUsableProduceMagic = RecordBatch.CURRENT_MAGIC_VALUE;

    // 更新指定节点的 API 版本信息，并重新计算最大可用的 Produce 协议版本号。
    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    // 从存储中移除指定节点的 API 版本信息，并重新计算最大可用的 Produce 协议版本号。
    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    // 获取指定节点的 API 版本信息。
    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    // 计算所有节点中支持的最小 Produce 协议版本号，以减少发送消息时的版本转换需求。
    private byte computeMaxUsableProduceMagic() {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.
        Optional<Byte> knownBrokerNodesMinRequiredMagicForProduce = this.nodeApiVersions.values().stream()
            .filter(versions -> versions.apiVersion(ApiKeys.PRODUCE) != null) // filter out Raft controller nodes
            .map(versions -> ProduceRequest.requiredMagicForVersion(versions.latestUsableVersion(ApiKeys.PRODUCE)))
            .min(Byte::compare);
        return (byte) Math.min(RecordBatch.CURRENT_MAGIC_VALUE,
            knownBrokerNodesMinRequiredMagicForProduce.orElse(RecordBatch.CURRENT_MAGIC_VALUE));
    }

    public synchronized byte maxUsableProduceMagic() {
        return maxUsableProduceMagic;
    }

}
