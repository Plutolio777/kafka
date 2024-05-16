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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Cluster;

import java.io.Closeable;

/**
 * Partitioner Interface
 * 分区器的顶级接口 用于Producer发送消息时选择对应的分区
 */
public interface Partitioner extends Configurable, Closeable {

    /**
     * Compute the partition for the given record.
     * 这个方法定义了分区的逻辑，用于决定给定记录应该被分配到哪个分区。
     * @param topic The topic name 分配记录的目标主题名称。
     * @param key The key to partition on (or null if no key) 用于分区的键（如果不存在键则为null）。
     * @param keyBytes The serialized key to partition on( or null if no key) 用于分区的序列化键（如果不存在键则为null）。
     * @param value The value to partition on or null 用于分区的值（或为null）。
     * @param valueBytes The serialized value to partition on or null 用于分区的序列化值（或为null）。
     * @param cluster The current cluster metadata 当前集群的元数据，包含分区和 broker 的信息。
     * @return int 返回分配给给定记录的分区编号。
     */
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);


    /**
     * This is called when partitioner is closed.
     */
    void close();

    /**
     * 通知分区器即将创建一个新的批次。当使用粘性分区器时，此方法可以更改新批次选择的粘性分区。
     * 注意：此方法仅在DefaultPartitioner和UniformStickyPartitioner中实现，这两个分区器现已废弃。
     * 有关更多信息，请参阅KIP-794。
     *
     * @param topic 主题名称
     * @param cluster 当前集群元数据
     * @param prevPartition 之前为触发新批次记录选择的分区
     */
    @Deprecated
    default void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        // 此处为onNewBatch方法的默认实现，子类可以通过重写此方法来提供特定行为。
    }

}
