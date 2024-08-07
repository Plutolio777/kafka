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
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class SslChannelBuilder implements ChannelBuilder, ListenerReconfigurable {
    // mark 监听器名称
    private final ListenerName listenerName;
    // mark 是否为内部代理监听
    private final boolean isInterBrokerListener;
    // mark ssl工厂实例
    private SslFactory sslFactory;
    // mark 客户端服务端标志
    private Mode mode;
    // mark 配置
    private Map<String, ?> configs;
    // mark ssl主体匹配器
    private SslPrincipalMapper sslPrincipalMapper;
    private final Logger log;

    /**
     * 构建 SSL 通道构建器。仅提供 ListenerName
     * 对于服务器通道构建器，对于客户端通道构建器将为空。
     */
    public SslChannelBuilder(Mode mode,
                             ListenerName listenerName,
                             boolean isInterBrokerListener,
                             LogContext logContext) {
        // mark 表示服务端还是客户端
        this.mode = mode;
        // mark 监听器名称
        this.listenerName = listenerName;
        // mark
        this.isInterBrokerListener = isInterBrokerListener;
        this.log = logContext.logger(getClass());
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            // mark 保存配置
            this.configs = configs;
            // mark 解析ssl.principal.mapping.rules配置
            String sslPrincipalMappingRules = (String) configs.get(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG);
            if (sslPrincipalMappingRules != null)
                sslPrincipalMapper = SslPrincipalMapper.fromRules(sslPrincipalMappingRules);
            // mark 创建ssl工厂
            this.sslFactory = new SslFactory(mode, null, isInterBrokerListener);
            // mark 该方法内部会生成 sslEngineFactory 它的工作主要是用来创建用于SSL引擎
            this.sslFactory.configure(this.configs);
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        sslFactory.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        sslFactory.reconfigure(configs);
    }

    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        try {
            // mark 构建SSL传输层
            SslTransportLayer transportLayer = buildTransportLayer(sslFactory, id, key, metadataRegistry);
            // mark 使用SslAuthenticator作为认证器
            Supplier<Authenticator> authenticatorCreator = () ->
                new SslAuthenticator(configs, transportLayer, listenerName, sslPrincipalMapper);
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public void close() {
        if (sslFactory != null) sslFactory.close();
    }

    /**
     * 根据给定的参数构建SSL传输层。
     * 此方法用于创建一个SSL传输层实例，该实例封装了与SSL/TLS相关的网络通信逻辑。
     *
     * @param sslFactory 用于创建SSL引擎的工厂，它提供了SSL配置的定制。
     * @param id 传输层的标识符，用于区分不同的传输层实例。
     * @param key 选择键，表示通道在选择器中的注册状态，用于非阻塞I/O操作。
     * @param metadataRegistry 渠道元数据注册表，用于注册和访问渠道相关的元数据。
     * @return 创建的SSL传输层实例。
     * @throws IOException 如果在创建SSL传输层过程中发生I/O错误。
     */
    protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, ChannelMetadataRegistry metadataRegistry) throws IOException {
        // mark 获取挂载在SelectionKey的SocketChannel
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // mark 使用给定的SSL工厂创建SSL引擎，并基于此创建SSL传输层实例。
        return SslTransportLayer.create(id, key, sslFactory.createSslEngine(socketChannel.socket()), metadataRegistry);
    }

    /**
     * Note that client SSL authentication is handled in {@link SslTransportLayer}. This class is only used
     * to transform the derived principal using a {@link KafkaPrincipalBuilder} configured by the user.
     */
    private static class SslAuthenticator implements Authenticator {
        private final SslTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;
        private final ListenerName listenerName;

        private SslAuthenticator(Map<String, ?> configs, SslTransportLayer transportLayer, ListenerName listenerName, SslPrincipalMapper sslPrincipalMapper) {
            this.transportLayer = transportLayer;
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, sslPrincipalMapper);
            this.listenerName = listenerName;
        }
        /**
         * No-Op for plaintext authenticator
         */
        @Override
        public void authenticate() {}

        /**
         * Constructs Principal using configured principalBuilder.
         * @return the built principal
         */
        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
            // listenerName should only be null in Client mode where principal() should not be called
            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            SslAuthenticationContext context = new SslAuthenticationContext(
                    transportLayer.sslSession(),
                    clientAddress,
                    listenerName.value());
            return principalBuilder.build(context);
        }

        @Override
        public Optional<KafkaPrincipalSerde> principalSerde() {
            return principalBuilder instanceof KafkaPrincipalSerde ? Optional.of((KafkaPrincipalSerde) principalBuilder) : Optional.empty();
        }

        @Override
        public void close() throws IOException {
            if (principalBuilder instanceof Closeable)
                Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }

        /**
         * SslAuthenticator doesn't implement any additional authentication mechanism.
         * @return true
         */
        @Override
        public boolean complete() {
            return true;
        }
    }
}
