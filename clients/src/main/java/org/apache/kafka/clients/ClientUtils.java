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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public final class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, String clientDnsLookupConfig) {
        return parseAndValidateAddresses(urls, ClientDnsLookup.forConfig(clientDnsLookupConfig));
    }

    /**
     * 解析并验证给定URL列表中的地址信息。
     *
     * @param urls 包含服务器地址信息的字符串列表，每个字符串应为“主机:端口”的格式。
     * @param clientDnsLookup 客户端DNS查找策略，决定如何处理主机名的DNS解析。
     * @return 一个包含有效地址信息的InetSocketAddress列表。
     * @throws ConfigException 如果提供的URL无法解析成有效的地址或端口，或者列表为空。
     */
    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, ClientDnsLookup clientDnsLookup) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        // mark 遍历所有的url
        for (String url : urls) {
            // 跳过空或空字符串的URL
            if (url != null && !url.isEmpty()) {
                try {
                    // 解析URL中的主机和端口
                    String host = getHost(url);
                    Integer port = getPort(url);
                    // 如果无法解析出主机或端口，则抛出配置异常
                    if (host == null || port == null)
                        throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);

                    // 根据DNS查找策略处理主机名解析
                    if (clientDnsLookup == ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY) {
                        InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                        for (InetAddress inetAddress : inetAddresses) {
                            // 尝试将解析的主机名转换为规范主机名
                            String resolvedCanonicalName = inetAddress.getCanonicalHostName();
                            InetSocketAddress address = new InetSocketAddress(resolvedCanonicalName, port);
                            // 如果地址无法解析，则记录警告；否则，添加到结果列表中
                            if (address.isUnresolved()) {
                                log.warn("Couldn't resolve server {} from {} as DNS resolution of the canonical hostname {} failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolvedCanonicalName, host);
                            } else {
                                addresses.add(address);
                            }
                        }
                    } else {
                        // 直接解析主机名，不考虑规范主机名
                        InetSocketAddress address = new InetSocketAddress(host, port);
                        // 如果地址无法解析，则记录警告；否则，添加到结果列表中
                        if (address.isUnresolved()) {
                            log.warn("Couldn't resolve server {} from {} as DNS resolution failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, host);
                        } else {
                            addresses.add(address);
                        }
                    }

                } catch (IllegalArgumentException e) {
                    // 如果端口号无效，则抛出配置异常
                    throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                } catch (UnknownHostException e) {
                    // 如果主机名未知，则抛出配置异常
                    throw new ConfigException("Unknown host in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        // 如果最终没有解析出任何有效地址，则抛出配置异常
        if (addresses.isEmpty())
            throw new ConfigException("No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    /**
     * Create a new channel builder from the provided configuration.
     *
     * @param config client configs
     * @param time the time implementation
     * @param logContext the logging context
     *
     * @return configured ChannelBuilder based on the configs.
     */
    public static ChannelBuilder createChannelBuilder(AbstractConfig config, Time time, LogContext logContext) {
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);
        return ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT, config, null,
                clientSaslMechanism, time, true, logContext);
    }

    /**
     * 解析给定主机名或IP地址，返回一系列的 InetAddress 对象。
     * 此方法首先使用提供的 HostResolver 实例解析主机名，然后过滤出首选的地址。
     * 它主要适用于需要根据特定策略或优先级选择 InetAddress 的场景。
     *
     * @param host         要解析的主机名或IP地址。
     * @param hostResolver 用于实际解析主机的实例，允许自定义解析逻辑。
     * @return 包含解析出的 InetAddress 对象的列表，根据首选项进行过滤。
     * @throws UnknownHostException 如果无法解析主机名。
     */
    static List<InetAddress> resolve(String host, HostResolver hostResolver) throws UnknownHostException {
        // mark 使用hostResolver解析提供的host，获取所有可用的InetAddress对象
        InetAddress[] addresses = hostResolver.resolve(host);
        // mark 过滤并返回InetAddress列表
        List<InetAddress> result = filterPreferredAddresses(addresses);
        // mark 如果日志级别允许，记录解析结果
        if (log.isDebugEnabled()) {
            log.debug("Resolved host {} as {}", host, result.stream().map(i -> i.getHostAddress()).collect(Collectors.joining(",")));
        }
        // 返回过滤后的InetAddress列表
        return result;
    }


    /**
     * 这个方法是统一ip地址类型的
     * 返回包含`allAddresses`中的第一个地址和后续地址   的列表
     * 这是第一个地址的子类型。
     * 结果是所有返回的地址都是 IPv4 或 IPv6（InetAddress 有两个
     * 子类：Inet4Address 和 Inet6Address）。
     */
    static List<InetAddress> filterPreferredAddresses(InetAddress[] allAddresses) {
        List<InetAddress> preferredAddresses = new ArrayList<>();
        Class<? extends InetAddress> clazz = null;
        for (InetAddress address : allAddresses) {
            if (clazz == null) {
                clazz = address.getClass();
            }
            if (clazz.isInstance(address)) {
                preferredAddresses.add(address);
            }
        }
        return preferredAddresses;
    }
}
