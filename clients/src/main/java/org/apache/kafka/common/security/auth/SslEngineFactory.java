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
package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.Configurable;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.security.KeyStore;
import java.util.Map;
import java.util.Set;

/**
 * 插件接口，用于以自定义方式创建 <code>SSLEngine</code> 对象。
 * 例如，你可以使用此接口来自定义加载 SSL 上下文所需的密钥材料和信任材料。
 * 这与现有的 Java 安全提供者机制互补，后者允许用自定义提供者替换整个提供者。
 * 在仅需要更新 SSL 引擎的配置机制的场景中，此接口提供了覆盖默认实现的便利方法。
 */
public interface SslEngineFactory extends Configurable, Closeable {

    /**
     * 创建一个新的 <code>SSLEngine</code> 对象供客户端使用。
     *
     * @param peerHost               要使用的对端主机。如果启用了端点验证，这将在客户端模式下使用。
     * @param peerPort               要使用的对端端口。此参数仅作为提示，不用于验证。
     * @param endpointIdentification 客户端模式下的端点标识算法。
     * @return 新创建的 <code>SSLEngine</code> 对象。
     */
    SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification);

    /**
     * 创建一个新的 <code>SSLEngine</code> 对象供服务器使用。
     *
     * @param peerHost               要使用的对端主机。此参数仅作为提示，不用于验证。
     * @param peerPort               要使用的对端端口。此参数仅作为提示，不用于验证。
     * @return 新创建的 <code>SSLEngine</code> 对象。
     */
    SSLEngine createServerSslEngine(String peerHost, int peerPort);

    /**
     * 如果需要重建 <code>SSLEngine</code> 对象，则返回 true。当 <code>SslFactory</code> 重新配置 SSL 引擎时，
     * 将调用此方法。根据提供的 <i>nextConfigs</i> 新配置，此方法将决定是否需要重建底层的 <code>SSLEngine</code> 对象。
     * 如果此方法返回 true，<code>SslFactory</code> 将使用 <i>nextConfigs</i> 创建该对象的新实例，并在决定
     * 使用新对象处理 <i>new incoming connection</i> 请求之前执行其他检查。现有连接不会受到影响，不会看到
     * 重新配置过程中所做的任何更改。
     * <p>
     * 例如，如果实现依赖于基于文件的密钥材料，它可以检查文件是否与之前/最后加载的时间戳相比已更新，
     * 并返回 true。
     * </p>
     *
     * @param nextConfigs       我们希望使用的新配置。
     * @return                  只有在底层的 <code>SSLEngine</code> 对象需要重建时才返回 true。
     */
    boolean shouldBeRebuilt(Map<String, Object> nextConfigs);

    /**
     * 返回可能需要重新配置的配置项名称。
     *
     * @return 动态可重新配置的配置选项名称。
     */
    Set<String> reconfigurableConfigs();

    /**
     * 返回为该工厂配置的密钥库。
     *
     * @return 该工厂的密钥库，如果未配置密钥库，则返回 null。
     */
    KeyStore keystore();

    /**
     * 返回为该工厂配置的信任库。
     *
     * @return 该工厂的信任库，如果未配置信任库，则返回 null。
     */
    KeyStore truststore();
}