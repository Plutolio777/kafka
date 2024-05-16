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
 * An interface for converting bytes to objects.
 *
 * A class that implements this interface is expected to have a constructor with no parameters.
 * <p>
 * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
 *
 * @param <T> Type to be deserialized into.
 */
/**
 * Deserializer接口定义了如何从字节数据中反序列化出特定类型的对象。
 * 这个接口扩展了Closeable接口，以支持资源的释放。
 *
 * @param <T> 要反序列化的对象类型
 */
public interface Deserializer<T> extends Closeable {

    /**
     * 配置Deserializer。
     * 这个方法允许通过键值对的形式传递配置信息，并且可以区分这些配置是用于键还是值。
     *
     * @param configs 配置信息的键值对
     * @param isKey 指示配置是用于键还是值
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // 故意留空，子类可以根据需要重写此方法
    }

    /**
     * 从一个字节数组中反序列化出一个记录的值或对象。
     * 此方法应能够处理null输入，并且推荐实现能够在遇到null时返回一个值或null，而不是抛出异常。
     *
     * @param topic 与数据关联的主题
     * @param data 序列化的字节数据；可能为null
     * @return 反序列化的有类型数据；可能为null
     */
    T deserialize(String topic, byte[] data);

    /**
     * 从一个字节数组中反序列化出一个记录的值或对象。
     * 提供了额外的headers参数，以包含与记录相关联的元数据。
     * 默认实现是调用没有headers参数的deserialize方法。
     *
     * @param topic 与数据关联的主题
     * @param headers 与记录关联的头部信息；可能为空
     * @param data 序列化的字节数据；可能为null
     * @return 反序列化的有类型数据；可能为null
     */
    default T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    /**
     * 关闭Deserializer。
     * 这个方法必须是幂等的，因为它可能被多次调用。
     */
    @Override
    default void close() {
        // 故意留空，子类可以根据需要重写此方法来处理资源释放
    }
}

