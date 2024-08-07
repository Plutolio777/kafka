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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class ChannelBuilders {
    private static final Logger log = LoggerFactory.getLogger(ChannelBuilders.class);

    private ChannelBuilders() { }

    /**
     * 以客户端的身份创建通信通道
     * @param securityProtocol 安全协议
     * @param contextType 上下文类型，如果 `securityProtocol` 是 SASL_*，则必须为非空；否则将被忽略
     * @param config 客户端配置
     * @param listenerName 如果 contextType 是 SERVER，则为监听器名称；否则为 null
     * @param clientSaslMechanism 如果模式是 CLIENT，则为 SASL 机制；否则被忽略
     * @param time 时间工具类
     * @param saslHandshakeRequestEnable 标志，用于启用 Sasl 握手请求；仅对于 SASL
     *             互联 Broker 连接且互联 Broker 协议版本 < 0.10 禁用
     * @param logContext 日志上下文实例
     *
     * @return 配置好的 `ChannelBuilder`
     * @throws IllegalArgumentException 如果上述模式不变的条件不满足
     */
    public static ChannelBuilder clientChannelBuilder(
            SecurityProtocol securityProtocol,
            JaasContext.Type contextType,
            AbstractConfig config,
            ListenerName listenerName,
            String clientSaslMechanism,
            Time time,
            boolean saslHandshakeRequestEnable,
            LogContext logContext) {
        // mark  如果认证协议中需要增加认证协议则校验必要的配置 jaas类型(这里是Server)以及使用的安全机制
        // mark 如果确实将抛出异常
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (contextType == null)
                throw new IllegalArgumentException("`contextType` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
            if (clientSaslMechanism == null)
                throw new IllegalArgumentException("`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `" + securityProtocol + "`");
        }
        return create(securityProtocol, Mode.CLIENT, contextType, config, listenerName, false, clientSaslMechanism,
                saslHandshakeRequestEnable, null, null, time, logContext, null);
    }

    /**
     * 以服务端的身份创建通道
     * @param listenerName 监听器名称
     * @param isInterBrokerListener 是否为互联 Broker 请求使用的监听器
     * @param securityProtocol 安全协议
     * @param config 服务器配置
     * @param credentialCache 如果启用了 SCRAM，则用于 SASL/SCRAM 的凭据缓存
     * @param tokenCache 委托令牌缓存
     * @param time 时间实例
     * @param logContext 日志上下文实例
     * @param apiVersionSupplier 用于在认证之前发送 ApiVersions 响应的供应商
     *
     * @return 配置好的 `ChannelBuilder`
     */
    public static ChannelBuilder serverChannelBuilder(ListenerName listenerName,
                                                      boolean isInterBrokerListener,
                                                      SecurityProtocol securityProtocol,
                                                      AbstractConfig config,
                                                      CredentialCache credentialCache,
                                                      DelegationTokenCache tokenCache,
                                                      Time time,
                                                      LogContext logContext,
                                                      Supplier<ApiVersionsResponse> apiVersionSupplier) {
        return create(securityProtocol, Mode.SERVER, JaasContext.Type.SERVER, config, listenerName,
                isInterBrokerListener, null, true, credentialCache,
                tokenCache, time, logContext, apiVersionSupplier);
    }

    /**
     * 根据安全协议、模式和其他配置参数创建一个通道构建器。
     *
     * @param securityProtocol           安全协议，定义了通道的加密和身份验证方式。
     * @param mode                       模式，指示通道是运行在服务器端还是客户端。
     * @param contextType                JAAS上下文类型，用于SASL身份验证。
     * @param config                     配置参数，包含通道构建所需的详细配置。
     * @param listenerName               监听器名称，用于区分不同的网络监听端口。
     * @param isInterBrokerListener      标志位，表示该通道是否用于Broker之间的通信。
     * @param clientSaslMechanism        客户端使用的SASL机制，仅在SASL身份验证时需要。
     * @param saslHandshakeRequestEnable SASL握手请求是否启用，影响身份验证过程。
     * @param credentialCache            用于缓存凭据的对象，提高身份验证效率。
     * @param tokenCache                 用于缓存令牌的对象，主要用于Kerberos身份验证。
     * @param time                       提供当前时间的功能对象。
     * @param logContext                 日志上下文，用于记录通道构建过程中的日志信息。
     * @param apiVersionSupplier         提供API版本信息的供应商，用于兼容性检查。
     * @return 根据安全协议创建的特定类型的通道构建器。
     */
    private static ChannelBuilder create(SecurityProtocol securityProtocol,
                                         Mode mode,
                                         JaasContext.Type contextType,
                                         AbstractConfig config,
                                         ListenerName listenerName,
                                         boolean isInterBrokerListener,
                                         String clientSaslMechanism,
                                         boolean saslHandshakeRequestEnable,
                                         CredentialCache credentialCache,
                                         DelegationTokenCache tokenCache,
                                         Time time,
                                         LogContext logContext,
                                         Supplier<ApiVersionsResponse> apiVersionSupplier) {
        // mark 1.获取构建连接通道所需要的配置
        Map<String, Object> configs = channelBuilderConfigs(config, listenerName);

        ChannelBuilder channelBuilder;
        // mark 2.根据安全协议创建不同的通道对象
        switch (securityProtocol) {
            case SSL:
                // mark 提供了SSL相关的支持
                requireNonNullMode(mode, securityProtocol);
                channelBuilder = new SslChannelBuilder(mode, listenerName, isInterBrokerListener, logContext);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                requireNonNullMode(mode, securityProtocol);
                Map<String, JaasContext> jaasContexts;
                String sslClientAuthOverride = null;
                if (mode == Mode.SERVER) {
                    @SuppressWarnings("unchecked")
                    List<String> enabledMechanisms = (List<String>) configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
                    jaasContexts = new HashMap<>(enabledMechanisms.size());
                    for (String mechanism : enabledMechanisms)
                        jaasContexts.put(mechanism, JaasContext.loadServerContext(listenerName, mechanism, configs));

                    // SSL client authentication is enabled in brokers for SASL_SSL only if listener-prefixed config is specified.
                    if (listenerName != null && securityProtocol == SecurityProtocol.SASL_SSL) {
                        String configuredClientAuth = (String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                        String listenerClientAuth = (String) config.originalsWithPrefix(listenerName.configPrefix(), true)
                                .get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);

                        // If `ssl.client.auth` is configured at the listener-level, we don't set an override and SslFactory
                        // uses the value from `configs`. If not, we propagate `sslClientAuthOverride=NONE` to SslFactory and
                        // it applies the override to the latest configs when it is configured or reconfigured. `Note that
                        // ssl.client.auth` cannot be dynamically altered.
                        if (listenerClientAuth == null) {
                            sslClientAuthOverride = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT);
                            if (configuredClientAuth != null && !configuredClientAuth.equalsIgnoreCase(SslClientAuth.NONE.name())) {
                                log.warn("Broker configuration '{}' is applied only to SSL listeners. Listener-prefixed configuration can be used" +
                                        " to enable SSL client authentication for SASL_SSL listeners. In future releases, broker-wide option without" +
                                        " listener prefix may be applied to SASL_SSL listeners as well. All configuration options intended for specific" +
                                        " listeners should be listener-prefixed.", BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                            }
                        }
                    }
                } else {
                    // Use server context for inter-broker client connections and client context for other clients
                    JaasContext jaasContext = contextType == JaasContext.Type.CLIENT ? JaasContext.loadClientContext(configs) :
                            JaasContext.loadServerContext(listenerName, clientSaslMechanism, configs);
                    jaasContexts = Collections.singletonMap(clientSaslMechanism, jaasContext);
                }
                channelBuilder = new SaslChannelBuilder(mode,
                        jaasContexts,
                        securityProtocol,
                        listenerName,
                        isInterBrokerListener,
                        clientSaslMechanism,
                        saslHandshakeRequestEnable,
                        credentialCache,
                        tokenCache,
                        sslClientAuthOverride,
                        time,
                        logContext,
                        apiVersionSupplier);
                break;
            case PLAINTEXT:
                // mark 建议先看最简单的明文传输通道
                channelBuilder = new PlaintextChannelBuilder(listenerName);
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        // mark 配置通道构建器
        channelBuilder.configure(configs);
        return channelBuilder;
    }

    /**
     * 获取连接通道构建器所需要的配置 （主要工作就是解析针对listener.name级别生效的配置）
     * @return 一个可变的 RecordingMap。从 RecordingMap 获取的元素被标记为“已使用”。
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> channelBuilderConfigs(final AbstractConfig config, final ListenerName listenerName) {
        Map<String, Object> parsedConfigs;
        // mark 这里会构建配置 留了一个钩子  listener.name.{{name}}.props 可以指定监听器所需要的配置
        if (listenerName == null)
            parsedConfigs = (Map<String, Object>) config.values();
        else
            // mark 提取针对listener.name级别的配置并覆盖原始配置
            parsedConfigs = config.valuesWithPrefixOverride(listenerName.configPrefix());

        // mark 这里遍历一下原始的配置
        config.originals().entrySet().stream()
                // mark 排除已经被解析过的配置
            .filter(e -> !parsedConfigs.containsKey(e.getKey())) // exclude already parsed configs
                // mark listener.name.{{name}}.props 这种形式的key前面已经提取过了 所以没用了
            // exclude already parsed listener prefix configs
            .filter(e -> !(listenerName != null && e.getKey().startsWith(listenerName.configPrefix()) &&
                    parsedConfigs.containsKey(e.getKey().substring(listenerName.configPrefix().length()))))
                // mark listener.name.{{name}}.{anything}.props 这种在前面也提取过了 所以没用了
            // exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and key `some.prop` exists in parsed configs.
            .filter(e -> !(listenerName != null && parsedConfigs.containsKey(e.getKey().substring(e.getKey().indexOf('.') + 1))))
            // mark 添加到最终的配置中
            .forEach(e -> parsedConfigs.put(e.getKey(), e.getValue()));
        return parsedConfigs;
    }

    private static void requireNonNullMode(Mode mode, SecurityProtocol securityProtocol) {
        if (mode == null)
            throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
    }

    public static KafkaPrincipalBuilder createPrincipalBuilder(Map<String, ?> configs,
                                                               KerberosShortNamer kerberosShortNamer,
                                                               SslPrincipalMapper sslPrincipalMapper) {
        Class<?> principalBuilderClass = (Class<?>) configs.get(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        final KafkaPrincipalBuilder builder;

        if (principalBuilderClass == null || principalBuilderClass == DefaultKafkaPrincipalBuilder.class) {
            builder = new DefaultKafkaPrincipalBuilder(kerberosShortNamer, sslPrincipalMapper);
        } else if (KafkaPrincipalBuilder.class.isAssignableFrom(principalBuilderClass)) {
            builder = (KafkaPrincipalBuilder) Utils.newInstance(principalBuilderClass);
        } else {
            throw new InvalidConfigurationException("Type " + principalBuilderClass.getName() + " is not " +
                    "an instance of " + KafkaPrincipalBuilder.class.getName());
        }

        if (builder instanceof Configurable)
            ((Configurable) builder).configure(configs);

        return builder;
    }

}
