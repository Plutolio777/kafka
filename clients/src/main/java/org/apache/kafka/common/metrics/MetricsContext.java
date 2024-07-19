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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * MetricsContext 封装了通过 {@link org.apache.kafka.common.metrics.MetricsReporter} 暴露的指标的附加上下文标签信息。
 *
 * <p>{@link #contextLabels()} 映射提供以下信息：
 * <dl>
 *   <dt>在所有组件中</dt>
 *   <dd>一个 {@code _namespace} 字段，指示暴露指标的组件，例如 kafka.server、kafka.consumer。
 *   {@link JmxReporter} 使用这个字段作为 MBean 名称的前缀。</dd>
 *
 *   <dt>对于clients和streams</dt>
 *   <dd>通过客户端属性以 {@code metrics.context.<key>=<value>} 的形式传递的任意自由形式字段。</dd>
 *
 *   <dt>对于 Kafka Broker</dt>
 *   <dd>kafka.broker.id, kafka.cluster.id</dd>
 *
 *   <dt>对于 Connect Workers</dt>
 *   <dd>connect.kafka.cluster.id, connect.group.id</dd>
 * </dl>
 */
@InterfaceStability.Evolving
public interface MetricsContext {
    /* predefined fields */
    String NAMESPACE = "_namespace"; // metrics namespace, formerly jmx prefix

    /**
     * Returns the labels for this metrics context.
     *
     * @return the map of label keys and values; never null but possibly empty
     */
    Map<String, String> contextLabels();
}
