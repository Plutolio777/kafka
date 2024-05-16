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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;

import java.io.Closeable;
import java.util.Map;

/**
 * An interface for converting objects to bytes.
 *
 * A class that implements this interface is expected to have a constructor with no parameter.
 * <p>
 * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
 *
 * @param <T> Type to be serialized from.
 */
/**
 * Serializer接口定义了序列化器的行为，将特定类型的对象转换为字节数组。这个接口扩展了Closeable接口，以确保资源可以在不再需要时被释放。
 * @param <T> 要序列化的数据的类型。
 */
public interface Serializer<T> extends Closeable {

    /**
     * 配置序列化器。
     * 这个方法允许在使用序列化器之前对其进行配置，例如根据提供的配置参数调整序列化行为。这个方法是默认实现的，留空。
     * 创建实例的时候会调用
     * @param configs kafka生产者所有配置参数
     * @param isKey 指示序列化的是key还是value。
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // intentionally left blank
    }

    /**
     * 将数据转换为字节数组。
     * 这是序列化器的主要方法，它负责将给定的类型T的数据转换为字节数组，以便于在各种网络传输或存储中使用。
     * @param topic 与数据相关联的主题。这可以用于根据主题有不同的序列化策略。
     * @param data 待序列化的数据对象。
     * @return 序列化后的字节数组。
     */
    byte[] serialize(String topic, T data);

    /**
     * 将数据（包括相关联的headers）转换为字节数组。
     * 这个方法提供了一个额外的headers参数，允许在序列化过程中包含额外的元数据。默认实现是调用没有headers参数的serialize方法。
     * @param topic 与数据相关联的主题。
     * @param headers 与记录相关联的headers，包含额外的元数据。
     * @param data 待序列化的数据对象。
     * @return 序列化后的字节数组。
     */
    default byte[] serialize(String topic, Headers headers, T data) {
        return serialize(topic, data);
    }

    /**
     * 关闭序列化器。
     * 这个方法确保了序列化器在不再需要时可以释放任何持有的资源，例如关闭打开的文件句柄或者网络连接。这个方法的实现必须是幂等的，即可以被多次安全调用。
     */
    @Override
    default void close() {
        // intentionally left blank
    }
}
