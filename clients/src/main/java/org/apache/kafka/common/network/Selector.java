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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.internals.IntGaugeSuite;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 用于执行非阻塞多连接网络 I/O 的 nioSelector 接口。
 * <p>
 * 这个类与 {@link NetworkSend} 和 {@link NetworkReceive} 一起工作，用于传输大小限定的网络请求和响应。
 * <p>
 * 可以通过以下方式将连接添加到与整数ID关联的 nioSelector 中：
 *
 * <pre>
 * nioSelector.connect("42", new InetSocketAddress("google.com", server.port), 64000, 64000);
 * </pre>
 *
 * connect 方法在创建 TCP 连接时不会阻塞，因此仅开始初始化连接。成功调用此方法并不意味着已建立有效连接。
 * 发送请求、接收响应、处理连接完成和断开现有连接，都是使用 <code>poll()</code> 方法完成的。
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * nioSelector 维护了几个列表，这些列表在每次调用 <code>poll()</code> 后被重置，并可以通过各种 getter 方法访问。
 * 此类不是线程安全的！
 */
public class Selector implements Selectable, AutoCloseable {

    public static final long NO_IDLE_TIMEOUT_MS = -1;
    public static final int NO_FAILED_AUTHENTICATION_DELAY = 0;

    /**
     * 定义了关闭连接的模式。
     * <p>
     * 包含三种关闭模式：
     * 1. GRACEFUL：优雅关闭，处理所有待处理的缓冲接收，并通知断开连接。
     * 2. NOTIFY_ONLY：仅通知断开连接，丢弃所有待处理的接收。
     * 3. DISCARD_NO_NOTIFY：丢弃所有待处理的接收，不发送断开连接的通知。
     * <p>
     * 这些模式的主要区别在于它们如何处理待处理的接收以及是否通知断开连接。
     */
    private enum CloseMode {
        GRACEFUL(true),            // 优雅关闭，处理所有待处理的缓冲接收，并通知断开连接
        NOTIFY_ONLY(true),         // 仅通知断开连接，丢弃所有待处理的接收
        DISCARD_NO_NOTIFY(false);  // 丢弃所有待处理的接收，不通知断开连接

        /**
         * 标识是否通知断开连接。
         * 此字段确定在关闭过程中是否发送断开连接的通知。
         */
        boolean notifyDisconnect;

        /**
         * CloseMode 构造函数。
         * <p>
         * 初始化 notifyDisconnect 字段以确定在关闭模式中是否通知断开连接。
         *
         * @param notifyDisconnect 在实例化时指定是否通知断开连接。
         */
        CloseMode(boolean notifyDisconnect) {
            this.notifyDisconnect = notifyDisconnect;
        }
    }

    private final Logger log;
    // mark nio选择器
    private final java.nio.channels.Selector nioSelector;
    // mark 保存节点ID到Channel注册表
    private final Map<String, KafkaChannel> channels;
    // mark 直接指定通道需要静默的注册表
    private final Set<KafkaChannel> explicitlyMutedChannels;
    private boolean outOfMemory;
    // mark 发送完成的Send集合
    private final List<NetworkSend> completedSends;
    // mark 接收完成的Receive集合
    private final LinkedHashMap<String, NetworkReceive> completedReceives;
    // mark 在初始化连接就已经建立好的连接key
    private final Set<SelectionKey> immediatelyConnectedKeys;
    private final Map<String, KafkaChannel> closingChannels;
    private Set<SelectionKey> keysWithBufferedRead;
    // mark 断开连接的节点集合
    private final Map<String, ChannelState> disconnected;
    // mark 连接成功的节点集合
    private final List<String> connected;
    // mark 发送失败的请求集合
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    // mark 用来构建 KafkaChannel 的工具类
    private final ChannelBuilder channelBuilder;
    // mark 最大可以接收的数据量大小
    private final int maxReceiveSize;
    private final boolean recordTimePerConnection;
    // mark 空闲超时到期连接管理器
    private final IdleExpiryManager idleExpiryManager;
    private final LinkedHashMap<String, DelayedAuthenticationFailureClose> delayedClosingChannels;
    private final MemoryPool memoryPool;
    private final long lowMemThreshold;
    private final int failedAuthenticationDelayMs;

    //indicates if the previous call to poll was able to make progress in reading already-buffered data.
    //this is used to prevent tight loops when memory is not available to read any more data
    private boolean madeReadProgressLastPoll = true;

    /**
     * 创建一个Selector实例，用于管理网络连接和数据传输。
     *
     * @param maxReceiveSize       接收缓冲区的最大大小。
     * @param connectionMaxIdleMs  连接的最大闲置时间，单位毫秒。
     * @param failedAuthenticationDelayMs  当认证失败时，延迟关闭连接的时间，单位毫秒。
     * @param metrics             用于收集和记录指标的对象。
     * @param time                提供当前时间和其他时间功能的对象。
     * @param metricGrpPrefix     指标组的前缀。
     * @param metricTags          指标标签，用于区分不同的指标实例。
     * @param metricsPerConnection 是否为每个连接单独收集指标。
     * @param recordTimePerConnection 是否记录每个连接的时间。
     * @param channelBuilder      用于构建和配置通道的对象。
     * @param memoryPool          提供内存池用于分配内存。
     * @param logContext          日志上下文，用于生成日志。
     * @throws KafkaException 如果打开NIO选择器失败。
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    int failedAuthenticationDelayMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    boolean recordTimePerConnection,
                    ChannelBuilder channelBuilder,
                    MemoryPool memoryPool,
                    LogContext logContext) {
        // 尝试打开一个NIO选择器，如果失败则抛出KafkaException。
        // mark 创建Selector 所以kafka里面的Selector是对nio的一个业务包装
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        // mark 最大接收缓冲器大小
        this.maxReceiveSize = maxReceiveSize;
        // mark 时间工具类
        this.time = time;
        // mark 通道注册表
        this.channels = new HashMap<>();
        // mark 通道集合
        this.explicitlyMutedChannels = new HashSet<>();
        // mark 是否OOM
        this.outOfMemory = false;
        // mark 发送完成记录集合
        this.completedSends = new ArrayList<>();
        // mark 接收完成记录集合
        this.completedReceives = new LinkedHashMap<>();
        // mark nio<SelectionKey>集合
        this.immediatelyConnectedKeys = new HashSet<>();
        // mark 关闭的通道注册表
        this.closingChannels = new HashMap<>();
        //
        this.keysWithBufferedRead = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new HashMap<>();
        this.failedSends = new ArrayList<>();
        this.log = logContext.logger(Selector.class);
        this.sensors = new SelectorMetrics(metrics, metricGrpPrefix, metricTags, metricsPerConnection);
        // mark 通道构建器 主要是生成 kafka channel
        this.channelBuilder = channelBuilder;
        this.recordTimePerConnection = recordTimePerConnection;

        // 根据最大闲置时间配置IdleExpiryManager，如果没有配置则为null。
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);

        this.memoryPool = memoryPool;
        // 低内存阈值设置为内存池大小的10%。
        this.lowMemThreshold = (long) (0.1 * this.memoryPool.size());
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;

        // 根据认证失败延迟配置DelayedAuthenticationFailureClose映射，如果没有配置则为null。
        this.delayedClosingChannels = (failedAuthenticationDelayMs > NO_FAILED_AUTHENTICATION_DELAY) ? new LinkedHashMap<String, DelayedAuthenticationFailureClose>() : null;
    }

    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    boolean recordTimePerConnection,
                    ChannelBuilder channelBuilder,
                    MemoryPool memoryPool,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags,
                metricsPerConnection, recordTimePerConnection, channelBuilder, memoryPool, logContext);
    }

    /**
     * 构造函数的重载版本，用于创建一个Selector实例。
     * 这个构造函数通过提供额外的参数，允许对Metrics和MemoryPool进行更细致的配置。
     *
     * @param maxReceiveSize              接收缓冲区的最大大小。
     * @param connectionMaxIdleMs         连接的最大闲置时间。
     * @param failedAuthenticationDelayMs 失败认证后的延迟时间。
     * @param metrics                     用于记录Metrics的对象。
     * @param time                        提供时间功能的对象。
     * @param metricGrpPrefix             Metrics组的前缀。
     * @param metricTags                  Metrics的标签。
     * @param metricsPerConnection        是否为每个连接单独记录Metrics。
     * @param channelBuilder              用于构建Channel的策略。
     * @param logContext                  日志上下文。
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    int failedAuthenticationDelayMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, false, channelBuilder, MemoryPool.NONE, logContext);
    }

    /**
     * 构造函数的重载版本，用于创建一个Selector实例。
     *
     * @param maxReceiveSize       接收缓冲区的最大大小。
     * @param connectionMaxIdleMs  连接的最大闲置时间。
     * @param metrics              用于记录指标的对象。
     * @param time                 提供时间功能的对象。
     * @param metricGrpPrefix      指标组的前缀。
     * @param metricTags           指标标签的映射。
     * @param metricsPerConnection 是否为每个连接单独记录指标。
     * @param channelBuilder       用于构建通道的对象。
     * @param logContext           日志上下文。
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, channelBuilder, logContext);
    }


    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    public Selector(long connectionMaxIdleMS, int failedAuthenticationDelayMs, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, Collections.<String, String>emptyMap(), true, channelBuilder, logContext);
    }

    /**
     * 开始连接到指定地址，并将该连接添加到与给定ID关联的 nioSelector 中。
     * <p>
     * 请注意，此调用仅启动连接，连接将在未来的 {@link #poll(long)} 调用中完成。
     * 请检查 {@link #connected()} 以查看在给定的 poll 调用后哪些连接（如果有）已经完成。
     * @param id 新连接的ID
     * @param address 要连接的地址
     * @param sendBufferSize 新连接的发送缓冲区大小
     * @param receiveBufferSize 新连接的接收缓冲区大小
     * @throws IllegalStateException 如果该ID已经存在连接
     * @throws IOException 如果主机名的DNS解析失败或代理不可用
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        ensureNotRegistered(id);
        // mark 创建一个 socket channel
        SocketChannel socketChannel = SocketChannel.open();
        SelectionKey key = null;
        try {
            // mark 配置一下通道
            configureSocketChannel(socketChannel, sendBufferSize, receiveBufferSize);
            // mark 建立连接(该方法是异步连接 但是如果返回true则为立即连接)
            boolean connected = doConnect(socketChannel, address);

            // mark 这里做了以下几件事情
            // mark 1.将socket注册到 nio.selector中
            // mark 2.将kafka channel
            // mark 3.将kafka channel挂载到key上
            key = registerChannel(id, socketChannel, SelectionKey.OP_CONNECT);
            // mark 如果连接立即建立成功
            if (connected) {
                // OP_CONNECT won't trigger for immediately connected channels
                log.debug("Immediately connected to node {}", id);
                // mark connected 为true说明已经立即注册成功 则将SelectionKey添加到立即连接成功集合中
                immediatelyConnectedKeys.add(key);
                // mark 通道如果立即建立完毕 就必须不需要再关注连接时间了 重置兴趣键
                key.interestOps(0);
            }
        } catch (IOException | RuntimeException e) {
            if (key != null)
                immediatelyConnectedKeys.remove(key);
            channels.remove(id);
            socketChannel.close();
            throw e;
        }
    }

    /**
     * 尝试连接到指定的地址。此方法可被测试用例重写，以实现自定义的连接行为，
     * 特别是用于实现阻塞连接以模拟“立即连接”的套接字。
     *
     * @param channel 要连接的 SocketChannel 实例
     * @param address 目标地址
     * @return 如果连接成功，则返回 true；否则返回 false
     * @throws IOException 如果地址无法解析或发生其他 I/O 错误
     */
    protected boolean doConnect(SocketChannel channel, InetSocketAddress address) throws IOException {
        try {
            // mark 连接地址
            return channel.connect(address);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Can't resolve address: " + address, e);
        }
    }

    /**
     * 配置给定的 SocketChannel 实例，以进行非阻塞操作并设置相关的 Socket 参数。
     * <p>
     * 此方法将 SocketChannel 设置为非阻塞模式，并根据提供的缓冲区大小配置发送和接收缓冲区。
     * 还会启用 TCP Keep-Alive 选项并禁用 Nagle 算法（通过设置 TCP_NODELAY）。
     *
     * @param socketChannel 要配置的 SocketChannel 实例
     * @param sendBufferSize 发送缓冲区的大小。如果值为 {@link Selectable#USE_DEFAULT_BUFFER_SIZE}，则使用默认值
     * @param receiveBufferSize 接收缓冲区的大小。如果值为 {@link Selectable#USE_DEFAULT_BUFFER_SIZE}，则使用默认值
     * @throws IOException 如果在配置 SocketChannel 时发生 I/O 错误
     */
    private void configureSocketChannel(SocketChannel socketChannel, int sendBufferSize, int receiveBufferSize)
            throws IOException {
        // mark 非阻塞模式
        socketChannel.configureBlocking(false);
        // mark 开启TCP KEEP ALIVE
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);
        // mark 设置接收发送缓冲区大小
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);
        // mark 开启TCP NO DELAY
        socket.setTcpNoDelay(true);
    }

    /**
     * 将 nioSelector 注册到现有通道
     * 当连接被不同线程接受但由选择器处理时，在服务器端使用此选项
     * <p>
     * 如果`channels`或`closeChannels`中已经存在具有相同连接ID的连接，
     * 抛出异常。必须选择连接 ID 以避免重用远程端口时发生冲突。
     * Kafka Brokers 在连接 ID 中添加一个递增索引，以避免在计时窗口中重复使用
     * 当新连接建立时，现有连接可能尚未被代理关闭
     * 处理相同的远程主机：端口。
     *</p><p>
     * 如果无法为此连接创建`KafkaChannel`，则`socketChannel`将被关闭
     * 且其选择键被取消。
     * </p>
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        // mark 当前id是否是已经连接或者正在关闭连接 如果是则抛出异常
        ensureNotRegistered(id);
        // mark 注册到nio selector中并关注读事件
        registerChannel(id, socketChannel, SelectionKey.OP_READ);

        // mark 指标传感器记录
        this.sensors.connectionCreated.record();
        // Default to empty client information as the ApiVersionsRequest is not
        // mandatory. In this case, we still want to account for the connection.

        // mark 通道元数据注册表中注册一个空的客户端信息
        ChannelMetadataRegistry metadataRegistry = this.channel(id).channelMetadataRegistry();

        if (metadataRegistry.clientInformation() == null)
            metadataRegistry.registerClientInformation(ClientInformation.EMPTY);
    }

    private void ensureNotRegistered(String id) {
        // mark 当前id是否是已经连接或者正在关闭连接 如果是则抛出异常
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);
        if (this.closingChannels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id + " that is still being closed");
    }

    /**
     * 注册给定的 SocketChannel 到 nioSelector 中，并将其相关的 SelectionKey 返回。
     * 此方法还会创建并附加一个 KafkaChannel 实例，并将其与给定的 ID 相关联。
     * 如果 idleExpiryManager 已初始化，则更新该通道的过期时间。
     *
     * @param id 通道的唯一标识符
     * @param socketChannel 要注册的 SocketChannel 实例
     * @param interestedOps 选择操作的位掩码（如 SelectionKey.OP_READ, SelectionKey.OP_WRITE 等）
     * @return 注册的 SelectionKey 实例
     * @throws IOException 如果在注册通道时发生 I/O 错误
     */
    protected SelectionKey registerChannel(String id, SocketChannel socketChannel, int interestedOps) throws IOException {
        // mark 将socketChannel注册到nioSelector中 获取到这个channel的SelectionKey
        SelectionKey key = socketChannel.register(nioSelector, interestedOps);

        // mark 创建一个包装类并且作为附件挂在到SelectionKey上
        KafkaChannel channel = buildAndAttachKafkaChannel(socketChannel, id, key);
        // mark 在Kafka Selector中注册Kafka通道
        this.channels.put(id, channel);

        if (idleExpiryManager != null)
            idleExpiryManager.update(channel.id(), time.nanoseconds());
        return key;
    }

    /**
     * 创建并附加一个 KafkaChannel 实例到给定的 SelectionKey 上。
     * 如果创建 KafkaChannel 失败，则关闭与之关联的 SocketChannel 并取消 SelectionKey。
     *
     * @param socketChannel 要为其创建 KafkaChannel 的 SocketChannel 实例
     * @param id 通道的唯一标识符
     * @param key 与 SocketChannel 关联的 SelectionKey 实例
     * @return 创建并附加的 KafkaChannel 实例
     * @throws IOException 如果创建 KafkaChannel 失败，且在关闭 SocketChannel 或取消 SelectionKey 时发生 I/O 错误
     */
    private KafkaChannel buildAndAttachKafkaChannel(SocketChannel socketChannel, String id, SelectionKey key) throws IOException {
        try {
            // mark 创建一个KafkaChannel对象包装
            // mark channelBuilder 目前有四个 明文 PLAINTEXT SASL_SSL SASL_PLAINTEXT
            KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize, memoryPool,
                new SelectorChannelMetadataRegistry());
            // mark 然后在把channel绑定到SelectionKey中作为附件
            key.attach(channel);
            return channel;
        } catch (Exception e) {
            try {
                socketChannel.close();
            } finally {
                key.cancel();
            }
            throw new IOException("Channel could not be created for socket " + socketChannel, e);
        }
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        Utils.closeAllQuietly(firstException, "release connections",
                connections.stream().map(id -> (AutoCloseable) () -> close(id)).toArray(AutoCloseable[]::new));
        // If there is any exception thrown in close(id), we should still be able
        // to close the remaining objects, especially the sensors because keeping
        // the sensors may lead to failure to start up the ReplicaFetcherThread if
        // the old sensors with the same names has not yet been cleaned up.
        Utils.closeQuietly(nioSelector, "nioSelector", firstException);
        Utils.closeQuietly(sensors, "sensors", firstException);
        Utils.closeQuietly(channelBuilder, "channelBuilder", firstException);
        Throwable exception = firstException.get();
        if (exception instanceof RuntimeException && !(exception instanceof SecurityException)) {
            throw (RuntimeException) exception;
        }
    }

    /**
     * 将给定的请求排队，以便在随后的 {@link #poll(long)} 调用中发送
     * @param send 要发送的请求
     */
    public void send(NetworkSend send) {
        // mark 获取连接ID
        String connectionId = send.destinationId();

        // mark 获取通道（正常或者正在关闭）
        KafkaChannel channel = openOrClosingChannelOrFail(connectionId);

        // mark 如果通道正在关闭则添加到发送失败队列
        if (closingChannels.containsKey(connectionId)) {
            // ensure notification via `disconnected`, leave channel in the state in which closing was triggered
            this.failedSends.add(connectionId);

        } else {
            try {
                // mark 做两件事  把Send挂载到kafka channel上 然后把传输层中的真正的socket添加可写时间
                channel.setSend(send);
            } catch (Exception e) {
                // update the state for consistency, the channel will be discarded after `close`
                // mark 如果失败了则把通道设置为发送失败状态
                channel.state(ChannelState.FAILED_SEND);
                // ensure notification via `disconnected` when `failedSends` are processed in the next poll
                // mark 添加到发送失败通道集合中
                this.failedSends.add(connectionId);
                // mark 关闭通道
                close(channel, CloseMode.DISCARD_NO_NOTIFY);
                if (!(e instanceof CancelledKeyException)) {
                    log.error("Unexpected exception during send, closing connection {} and rethrowing exception {}",
                            connectionId, e);
                    throw e;
                }
            }
        }
    }

    /**
     * 在每个连接上执行可以完成的 I/O 操作而不阻塞。这包括完成连接、完成断开连接、发起新的发送请求，或在进行中的发送或接收操作上取得进展。
     *
     * 当此调用完成后，用户可以使用 {@link #completedSends()}、{@link #completedReceives()}、{@link #connected()}、{@link #disconnected()} 检查已完成的发送、接收、连接或断开连接。这些列表将在每次 `poll` 调用开始时被清空，并在调用中重新填充，如果有任何已完成的 I/O。
     *
     * 在 "Plaintext" 设置中，我们使用 `socketChannel` 进行网络的读写。但在 "SSL" 设置中，我们在使用 `socketChannel` 写入数据到网络之前对数据进行加密，并在返回响应之前进行解密。这需要在读取网络数据时维护额外的缓冲区，因为网络上的数据是加密的，我们无法读取出 Kafka 协议所需的确切字节数。我们读取尽可能多的字节，最多到 SSLEngine 的应用缓冲区大小。这意味着我们可能会读取比请求大小更多的字节。如果 `socketChannel` 没有更多数据可读，选择器不会调用该通道，而我们在缓冲区中会有多余的字节。为了解决这个问题，我们添加了 `keysWithBufferedRead` 映射，它跟踪在 SSL 缓冲区中有数据的通道。如果有可以处理的缓冲数据通道，我们将 "timeout" 设置为 0，即使没有更多数据可读，我们也会处理这些数据。
     *
     * 在每次轮询中，对于一个通道，最多只会添加一个条目到 "completedReceives" 中。这是为了保证通道中的请求按照发送顺序在代理上处理。由于 SocketServer 添加到请求队列中的待处理请求可能会被不同的请求处理线程处理，为了保证顺序，每个通道的请求必须逐个处理。
     *
     * @param timeout 等待的时间，单位为毫秒，必须是非负值
     * @throws IllegalArgumentException 如果 `timeout` 为负值
     * @throws IllegalStateException 如果发送的请求没有现有连接，或已存在一个正在进行的发送
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        boolean madeReadProgressLastCall = madeReadProgressLastPoll;
        clear();

        boolean dataInBuffers = !keysWithBufferedRead.isEmpty();

        if (!immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
            timeout = 0;

        // mark 取消通道静默（如果内存池没有OOM 但是Select oom标志位为内存溢出 说明OOM可能已经解除需要尝试取消通道静默）
        if (!memoryPool.isOutOfMemory() && outOfMemory) {
            //we have recovered from memory pressure. unmute any channel not explicitly muted for other reasons
            log.trace("Broker no longer low on memory - unmuting incoming sockets");
            for (KafkaChannel channel : channels.values()) {
                //  mark 1.通道为可修改状态 2.指定静音通道注册表不包含该通道 则尝试取消静音
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    // mark 尝试取消通道静默
                    channel.maybeUnmute();
                }
            }
            outOfMemory = false;
        }

        /* check ready keys */
        long startSelect = time.nanoseconds();
        // mark 轮询选择器
        int numReadyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        // mark 传感器记录指标
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds(), false);

        // mark 开始处理的条件
        // mark 1.nio.selector 监听到IO事件
        // mark 2.连接建立时立刻连接好的节点 (立刻连接的节点关注的事件是0， 不会出现在numReadyKeys中)
        // mark 3.存在混存数据
        if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {

            Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

            // mark 1.先处理通道上有数据缓存的key 这里需要将这些key从selector监听到的keys进行移除 以避免从孵出来
            // mark 2.在SSL连接的时候才有可能出现数据缓存
            if (dataInBuffers) {
                // mark 避免重复处理
                keysWithBufferedRead.removeAll(readyKeys); //so no channel gets polled twice
                // mark 重建集合
                Set<SelectionKey> toPoll = keysWithBufferedRead;
                keysWithBufferedRead = new HashSet<>(); //poll() calls will repopulate if needed
                // mark 按照常规处理SelectionKey的方式进行处理
                pollSelectionKeys(toPoll, false, endSelect);
            }

            // mark 2.处理selector监听到IO时间的keys
            pollSelectionKeys(readyKeys, false, endSelect);
            // Clear all selected keys so that they are included in the ready count for the next select
            readyKeys.clear();

            // mark 3.处理立即连接好的keys
            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
            immediatelyConnectedKeys.clear();
        } else {

            madeReadProgressLastPoll = true; //no work is also "progress"
        }

        long endIo = time.nanoseconds();
        // mark 记录IO处理的延时时间
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds(), false);

        // Close channels that were delayed and are now ready to be closed
        completeDelayedChannelClose(endIo);

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        maybeCloseOldestConnection(endSelect);
    }

    /**
     * 处理selectionKeys事件
     * @param selectionKeys 要处理的键集合
     * @param isImmediatelyConnected 如果在刚刚连接的套接字上运行，则为 true
     * @param currentTimeNanos 确定键集合的时间（纳秒级别）
     */
    // package-private for testing
    void pollSelectionKeys(Set<SelectionKey> selectionKeys,
                           boolean isImmediatelyConnected,
                           long currentTimeNanos) {
        // mark 开始遍历selectionKeys determineHandlingOrder （为防止请求顺序固定时导致阻塞 这里需要对请求顺序进行洗牌）
        for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
            /** 从key绑定的附件上获取kafka channel {@link Selector#buildAndAttachKafkaChannel}  */
            KafkaChannel channel = channel(key);


            long channelStartTimeNanos = recordTimePerConnection ? time.nanoseconds() : 0;
            boolean sendFailed = false;
            // mark 获取通道的节点ID
            String nodeId = channel.id();

            // mark 一次注册所有每个连接的指标 (注册有关连接通道的相关传感器和指标)
            sensors.maybeRegisterConnectionMetrics(nodeId);

            // mark 更新连接到空闲超时到期连接管理器中，并记录活跃时间 实际是记录通道在LRU中的活跃程度
            if (idleExpiryManager != null)
                idleExpiryManager.update(nodeId, currentTimeNanos);


            try {
                /**
                 * 完成所有已完成握手的连接（正常或立即{@link Selector#connect}）
                 * 连接时间处理流程如下：
                 * 1.调整 {@link KafkaChannel#remoteAddress} 为socket channel的远程地址
                 * 2.调用 {@link SocketChannel#finishConnect} 方法确认连接是否真正建立
                 * 3.调用传输层的{@link TransportLayer#finishConnect}方法取消关注连接事件增加关注可读事件
                 *      具体可见：
                 *          SSL传输层 {@link SslTransportLayer#finishConnect}
                 *          明文传输层 {@link PlaintextTransportLayer#finishConnect}
                 * 4.修改kafka通道的状态 {@link ChannelState}
                 *      具体可见：
                 *          1.如果传输层就绪，认证完毕则通道状态为  {@link ChannelState#READY}
                 *          2.否则通道状态为 {@link ChannelState#AUTHENTICATE}
                 * 5.将节点注册到已连接记录中 {@link Selector#connected}
                 * 6.记录连接创建状态
                 * 7.打印连接记录日志
                 */
                if (isImmediatelyConnected || key.isConnectable()) {
                    // mark 判断通道是否真正连接成功 （isConnectable 只能判断连接过程是否已经结束 但是连接结果不得而知）
                    if (channel.finishConnect()) {
                        // mark 连接成功 添加到连接成功队列中
                        this.connected.add(nodeId);
                        // mark 记录指标
                        this.sensors.connectionCreated.record();
                        // mark 打印日志
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
                                nodeId);
                    } else {
                        // mark 连接没有建立 后面再检查
                        continue;
                    }
                }

                /**
                 * 如果连接已建立 但是通道还未准备就绪 则需要调用 {@link KafkaChannel#prepare} 方法准备通道
                 * 具体流程如下：
                 * 1.调用 {@link KafkaChannel#prepare}方法尝试握手和认证
                 *      具体可见：
                 *          1.如果传输层未就绪则调用 {@link TransportLayer#handshake} 方法进行握手操作
                 *              SSL传输层 {@link SslTransportLayer#handshake}
                 *              明文传输层 {@link PlaintextTransportLayer#handshake}
                 *          2.如果还未进行认证则调用 {@link Authenticator#authenticate} 方法进行认证
                 * 2.记录相关指标
                 */
                if (channel.isConnected() && !channel.ready()) {
                    // mark 尝试进行连接层握手操作和认证操作
                    channel.prepare();
                    if (channel.ready()) {
                        // mark  记录通道完备时间戳
                        long readyTimeMs = time.milliseconds();
                        // mark 记录通道是否是重复认证
                        boolean isReauthentication = channel.successfulAuthentications() > 1;
                        if (isReauthentication) {
                            sensors.successfulReauthentication.record(1.0, readyTimeMs);
                            if (channel.reauthenticationLatencyMs() == null)
                                log.warn(
                                    "Should never happen: re-authentication latency for a re-authenticated channel was null; continuing...");
                            else
                                sensors.reauthenticationLatency
                                    .record(channel.reauthenticationLatencyMs().doubleValue(), readyTimeMs);
                        } else {
                            sensors.successfulAuthentication.record(1.0, readyTimeMs);
                            if (!channel.connectedClientSupportsReauthentication())
                                sensors.successfulAuthenticationNoReauth.record(1.0, readyTimeMs);
                        }
                        log.debug("Successfully {}authenticated with {}", isReauthentication ?
                            "re-" : "", channel.socketDescription());
                    }
                }

                /** 如果通道准备好 但是通道状态是未连接 强行调整为READY */
                if (channel.ready() && channel.state() == ChannelState.NOT_CONNECTED)
                    channel.state(ChannelState.READY);

                Optional<NetworkReceive> responseReceivedDuringReauthentication = channel.pollResponseReceivedDuringReauthentication();

                responseReceivedDuringReauthentication.ifPresent(receive -> {
                    long currentTimeMs = time.milliseconds();
                    addToCompletedReceives(channel, receive, currentTimeMs);
                });

                /**
                 * 尝试从通道读取数据，需满足以下条件：
                 * 1.通道就绪 （传输层就绪，认证通过）
                 * 2.key为可读事件或者通道中的传输层存在缓存数据（仅限SSL连接）
                 * 3.{@link Selector#completedReceives}中不包含该通道
                 * 4.通道并未强制静音 即{@link Selector#explicitlyMutedChannels}
                 * 具体读取流程如下：
                 *     1.调用kafka channel的read方法读取数据 {@link KafkaChannel#read()}
                 *         1.生成或者复用 {@link KafkaChannel#receive}对象
                 *         2.调用 {@link NetworkReceive#readFrom(ScatteringByteChannel)} 从传输层通道中获取数据
                 *             1.从通道中读取4个字节作为接收数据大小 {@link NetworkReceive#size}
                 *             2.根据接收到的数据大小像内存池 {@link NetworkReceive#memoryPool} 申请buffer {@link NetworkReceive#buffer}
                 *             3.使用buffer向通道中后驱剩余数据
                 *         3.判断是否存在内存无法分配的情况 如果是则满足静音条件下将通道静音
                 *     2.read过程中是否接收数据成功 如果成功则将接收到的数据保存在 {@link Selector#completedReceives} 并重置通道的receive
                 *     3.根据read的状态添加内存溢出标志或者读取进展标志
                 */
                if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasCompletedReceive(channel)
                        && !explicitlyMutedChannels.contains(channel)) {
                    attemptRead(channel);
                }

                /**
                 * 如果传输层存在数据缓存 并且通道没有强制静音 则将key添加到 {@link Selector#keysWithBufferedRead} 中后面一轮特别处理
                 *
                 */
                if (channel.hasBytesBuffered() && !explicitlyMutedChannels.contains(channel)) {
                    //this channel has bytes enqueued in intermediary buffers that we could not read
                    //(possibly because no memory). it may be the case that the underlying socket will
                    //not come up in the next poll() and so we need to remember this channel for the
                    //next poll call otherwise data may be stuck in said buffers forever. If we attempt
                    //to process buffered data and no progress is made, the channel buffered status is
                    //cleared to avoid the overhead of checking every time.
                    keysWithBufferedRead.add(key);
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */

                long nowNanos = channelStartTimeNanos != 0 ? channelStartTimeNanos : currentTimeNanos;
                try {
                    attemptWrite(key, channel, nowNanos);
                } catch (Exception e) {
                    sendFailed = true;
                    throw e;
                }

                /* 取消任何失效的套接字 */
                if (!key.isValid())
                    close(channel, CloseMode.GRACEFUL);

            }
            catch (Exception e) {
                String desc = String.format("%s (channelId=%s)", channel.socketDescription(), channel.id());
                // mark 处理IO异常
                if (e instanceof IOException) {
                    log.debug("Connection with {} disconnected", desc, e);
                // mark 处理认证异常
                } else if (e instanceof AuthenticationException) {

                    // mark 记录认证失败和重新认证失败的次数
                    boolean isReauthentication = channel.successfulAuthentications() > 0;
                    if (isReauthentication)
                        sensors.failedReauthentication.record();
                    else
                        sensors.failedAuthentication.record();

                    String exceptionMessage = e.getMessage();
                    if (e instanceof DelayedResponseAuthenticationException)
                        exceptionMessage = e.getCause().getMessage();
                    log.info("Failed {}authentication with {} ({})", isReauthentication ? "re-" : "",
                        desc, exceptionMessage);
                } else {
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                }

                if (e instanceof DelayedResponseAuthenticationException)
                    maybeDelayCloseOnAuthenticationFailure(channel);
                else
                    close(channel, sendFailed ? CloseMode.NOTIFY_ONLY : CloseMode.GRACEFUL);
            }
            finally {
                maybeRecordTimePerConnection(channel, channelStartTimeNanos);
            }
        }
    }

    /**
     * 尝试写入数据到指定的Kafka通道
     *
     * @param key       选择键，用于判断通道是否可写
     * @param channel   Kafka通道，用于数据的发送
     * @param nowNanos  当前时间的纳秒值，用于通道的重新认证判断
     * @throws IOException 如果写入操作失败，抛出此异常
     */
    private void attemptWrite(SelectionKey key, KafkaChannel channel, long nowNanos) throws IOException {
        /**
         * 数据发送满足条件
         * 1.channel上挂载有待发送数据 数据挂载参见 {@link org.apache.kafka.clients.NetworkClient#send} 和 {@link Selector#send}
         * 2.selector监听到通道发生可写事件
         * 3.通道不需要重新认证
         */
        if (channel.hasSend()
                && channel.ready()
                && key.isWritable()
                && !channel.maybeBeginClientReauthentication(() -> nowNanos)) {
            write(channel); // 满足条件时，调用write方法进行数据写入
        }
    }


    /**
     * 向指定的Kafka通道写入数据
     * 此方法负责记录数据发送的统计信息，包括发送的字节数和完成的发送事件
     *
     * @param channel Kafka通道，用于数据传输
     * @throws IOException 如果在写操作中出现IO错误
     */
    void write(KafkaChannel channel) throws IOException {
        String nodeId = channel.id();
        // mark 调用通道的写入方法写入数据
        long bytesSent = channel.write();
        // mark 确认通道是否完成发送
        NetworkSend send = channel.maybeCompleteSend();
        // We may complete the send with bytesSent < 1 if `TransportLayer.hasPendingWrites` was true and `channel.write()`
        // caused the pending writes to be written to the socket channel buffer

        if (bytesSent > 0 || send != null) {
            long currentTimeMs = time.milliseconds();
            if (bytesSent > 0)
                // mark 记录发送的大小以及时间
                this.sensors.recordBytesSent(nodeId, bytesSent, currentTimeMs);
            if (send != null) {
                // mark 将发送完成的数据添加到发送完成集合中
                this.completedSends.add(send);
                // mark 记录发送完成的大小以及时间
                this.sensors.recordCompletedSend(nodeId, send.size(), currentTimeMs);
            }
        }
    }

    /**
     * 确定处理SelectionKey的顺序以避免因固定顺序处理而导致的内存不足问题。
     * 当内存池中的可用内存低于某一阈值时，它会随机打乱键的处理顺序，
     * 防止在内存较低的情况下，某些键由于总是被排在其他键之后处理而可能产生的饥饿现象。
     *
     * @param selectionKeys 需要处理的SelectionKey集合。
     * @return 处理顺序确定后的SelectionKey集合，如果内存充足则直接返回原集合，
     *         如果内存低于阈值则返回一个被打乱顺序的新列表。
     */
    private Collection<SelectionKey> determineHandlingOrder(Set<SelectionKey> selectionKeys) {
        // mark 每次调用时，selectionKeys 上的迭代顺序可能是相同的。
        // mark 当内存不足时，这可能会导致读取饥饿。为了解决这个问题，如果内存不足，我们会打乱键。
        if (!outOfMemory && memoryPool.availableMemory() < lowMemThreshold) {
            List<SelectionKey> shuffledKeys = new ArrayList<>(selectionKeys);
            Collections.shuffle(shuffledKeys);
            return shuffledKeys;
        } else {
            return selectionKeys;
        }
    }

    /**
     * 尝试从给定的KafkaChannel读取数据。
     * 当有数据可读时，更新相关指标并处理接收到的数据。
     * 如果通道被静音，标记内存压力状态为true。
     *
     * @param channel 要尝试读取数据的KafkaChannel。
     * @throws IOException 如果读取操作失败。
     */
    private void attemptRead(KafkaChannel channel) throws IOException {
        // mark 获取通道的节点ID
        String nodeId = channel.id();

        // mark 从kafka channel 中读取数据，返回读取的字节数
        long bytesReceived = channel.read();
        // mark 如果bytesReceived不为0说明读取到了数据
        if (bytesReceived != 0) {
            long currentTimeMs = time.milliseconds();
            // mark 使用传感器记录接收的字节数和时间
            sensors.recordBytesReceived(nodeId, bytesReceived, currentTimeMs);
            // mark 标记此次轮询有读取进展
            madeReadProgressLastPoll = true;

            // mark 尝试完成接收操作，返回可能已完成的NetworkReceive对象
            // mark 如果接收未完成则后续还会继续接收
            NetworkReceive receive = channel.maybeCompleteReceive();
            if (receive != null) {
                // mark 如果有完成的接收操作，将其添加到已完成的接收列表中
                addToCompletedReceives(channel, receive, currentTimeMs);
            }
        }
        // mark 如果在read过程中通道被静音了 则说明内存池溢出添加标记
        if (channel.isMuted()) {
            // 如果是，标记内存压力状态为true
            outOfMemory = true; //channel has muted itself due to memory pressure.
        } else {
            // mark 如果不是，标记此次轮询有读取进展
            madeReadProgressLastPoll = true;
        }
    }


    private boolean maybeReadFromClosingChannel(KafkaChannel channel) {
        boolean hasPending;
        if (channel.state().state() != ChannelState.State.READY)
            hasPending = false;
        else if (explicitlyMutedChannels.contains(channel) || hasCompletedReceive(channel))
            hasPending = true;
        else {
            try {
                attemptRead(channel);
                hasPending = hasCompletedReceive(channel);
            } catch (Exception e) {
                log.trace("Read from closing channel failed, ignoring exception", e);
                hasPending = false;
            }
        }
        return hasPending;
    }

    // Record time spent in pollSelectionKeys for channel (moved into a method to keep checkstyle happy)
    private void maybeRecordTimePerConnection(KafkaChannel channel, long startTimeNanos) {
        if (recordTimePerConnection)
            channel.addNetworkThreadTimeNanos(time.nanoseconds() - startTimeNanos);
    }

    @Override
    public List<NetworkSend> completedSends() {
        return this.completedSends;
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return this.completedReceives.values();
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
        explicitlyMutedChannels.add(channel);
        keysWithBufferedRead.remove(channel.selectionKey());
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        // Remove the channel from explicitlyMutedChannels only if the channel has been actually unmuted.
        if (channel.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel);
            if (channel.hasBytesBuffered()) {
                keysWithBufferedRead.add(channel.selectionKey());
                madeReadProgressLastPoll = true;
            }
        }
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    // package-private for testing
    void completeDelayedChannelClose(long currentTimeNanos) {
        if (delayedClosingChannels == null)
            return;

        while (!delayedClosingChannels.isEmpty()) {
            DelayedAuthenticationFailureClose delayedClose = delayedClosingChannels.values().iterator().next();
            if (!delayedClose.tryClose(currentTimeNanos))
                break;
        }
    }

    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();
            KafkaChannel channel = this.channels.get(connectionId);
            if (channel != null) {
                if (log.isTraceEnabled())
                    log.trace("About to close the idle connection from {} due to being idle for {} millis",
                            connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);
                channel.state(ChannelState.EXPIRED);
                close(channel, CloseMode.GRACEFUL);
            }
        }
    }

    /**
     * Clears completed receives. This is used by SocketServer to remove references to
     * receive buffers after processing completed receives, without waiting for the next
     * poll().
     */
    public void clearCompletedReceives() {
        this.completedReceives.clear();
    }

    /**
     * Clears completed sends. This is used by SocketServer to remove references to
     * send buffers after processing completed sends, without waiting for the next
     * poll().
     */
    public void clearCompletedSends() {
        this.completedSends.clear();
    }

    /**
     * Clears all the results from the previous poll. This is invoked by Selector at the start of
     * a poll() when all the results from the previous poll are expected to have been handled.
     * <p>
     * SocketServer uses {@link #clearCompletedSends()} and {@link #clearCompletedReceives()} to
     * clear `completedSends` and `completedReceives` as soon as they are processed to avoid
     * holding onto large request/response buffers from multiple connections longer than necessary.
     * Clients rely on Selector invoking {@link #clear()} at the start of each poll() since memory usage
     * is less critical and clearing once-per-poll provides the flexibility to process these results in
     * any order before the next poll.
     */
    private void clear() {
        // mark 发送完成队列清空
        this.completedSends.clear();
        // mark 接收完成队列清空
        this.completedReceives.clear();
        // mark 已连接队列清空
        this.connected.clear();
        // mark 失去连接队列清空
        this.disconnected.clear();

       // mark 在处理完所有缓冲的接收后或请求发送后，删除关闭的通道
        for (Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator(); it.hasNext(); ) {
            KafkaChannel channel = it.next().getValue();
            boolean sendFailed = failedSends.remove(channel.id());
            boolean hasPending = false;
            if (!sendFailed)
                hasPending = maybeReadFromClosingChannel(channel);
            if (!hasPending) {
                doClose(channel, true);
                it.remove();
            }
        }

        for (String channel : this.failedSends)
            this.disconnected.put(channel, ChannelState.FAILED_SEND);
        this.failedSends.clear();
        this.madeReadProgressLastPoll = false;
    }

    /**
     * 检查数据，等待最长时间为指定的超时时间。
     *
     * @param timeoutMs 等待时间长度，以毫秒为单位，必须为非负数
     * @return 准备好的键的数量
     */
    private int select(long timeoutMs) throws IOException {
        if (timeoutMs < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (timeoutMs == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(timeoutMs);
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            // There is no disconnect notification for local close, but updating
            // channel state here anyway to avoid confusion.
            channel.state(ChannelState.LOCAL_CLOSE);
            close(channel, CloseMode.DISCARD_NO_NOTIFY);
        } else {
            KafkaChannel closingChannel = this.closingChannels.remove(id);
            // Close any closing channel, leave the channel in the state in which closing was triggered
            if (closingChannel != null)
                doClose(closingChannel, false);
        }
    }

    private void maybeDelayCloseOnAuthenticationFailure(KafkaChannel channel) {
        DelayedAuthenticationFailureClose delayedClose = new DelayedAuthenticationFailureClose(channel, failedAuthenticationDelayMs);
        if (delayedClosingChannels != null)
            delayedClosingChannels.put(channel.id(), delayedClose);
        else
            delayedClose.closeNow();
    }

    private void handleCloseOnAuthenticationFailure(KafkaChannel channel) {
        try {
            channel.completeCloseOnAuthenticationFailure();
        } catch (Exception e) {
            log.error("Exception handling close on authentication failure node {}", channel.id(), e);
        } finally {
            close(channel, CloseMode.GRACEFUL);
        }
    }

    /**
     * 开始关闭此连接。
     * 如果 'closeMode' 是 `CloseMode.GRACEFUL`，则此处断开通道，但未处理的接收将被处理。
     * 当没有未处理的接收或请求发送时，通道将关闭。对于 `closeMode` 的其他值，未处理的接收将被丢弃，并且通道将立即关闭。
     *
     * 如果 `closeMode.notifyDisconnect` 为 true，则通道在实际关闭时将被添加到断开连接列表中。
     */
    private void close(KafkaChannel channel, CloseMode closeMode) {
        channel.disconnect();

        // Ensure that `connected` does not have closed channels. This could happen if `prepare` throws an exception
        // in the `poll` invocation when `finishConnect` succeeds
        connected.remove(channel.id());

        // Keep track of closed channels with pending receives so that all received records
        // may be processed. For example, when producer with acks=0 sends some records and
        // closes its connections, a single poll() in the broker may receive records and
        // handle close(). When the remote end closes its connection, the channel is retained until
        // a send fails or all outstanding receives are processed. Mute state of disconnected channels
        // are tracked to ensure that requests are processed one-by-one by the broker to preserve ordering.
        if (closeMode == CloseMode.GRACEFUL && maybeReadFromClosingChannel(channel)) {
            closingChannels.put(channel.id(), channel);
            log.debug("Tracking closing connection {} to process outstanding requests", channel.id());
        } else {
            doClose(channel, closeMode.notifyDisconnect);
        }
        this.channels.remove(channel.id());

        if (delayedClosingChannels != null)
            delayedClosingChannels.remove(channel.id());

        if (idleExpiryManager != null)
            idleExpiryManager.remove(channel.id());
    }

    private void doClose(KafkaChannel channel, boolean notifyDisconnect) {
        SelectionKey key = channel.selectionKey();
        try {
            immediatelyConnectedKeys.remove(key);
            keysWithBufferedRead.remove(key);
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        } finally {
            key.cancel();
            key.attach(null);
        }

        this.sensors.connectionClosed.record();
        this.explicitlyMutedChannels.remove(channel);
        if (notifyDisconnect)
            this.disconnected.put(channel.id(), channel.state());
    }

    /**
     * check if channel is ready
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    /**
     * 尝试获取一个指定ID的Kafka通道，该通道可能是打开的或正在关闭的。
     * 如果找不到对应的通道，则抛出IllegalStateException。
     *
     * @param id 通道的唯一标识符。
     * @return 对应ID的Kafka通道。
     * @throws IllegalStateException 如果没有找到对应的通道。
     */
    private KafkaChannel openOrClosingChannelOrFail(String id) {
        // mark 尝试从打开的通道集合中获取通道
        KafkaChannel channel = this.channels.get(id);
        // mark 如果打开的通道中没有找到，尝试从正在关闭的通道集合中获取
        if (channel == null)
            channel = this.closingChannels.get(id);
        // mark 如果在打开的和正在关闭的通道集合中都找不到，抛出异常
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no connection. Connection id " + id + " existing connections " + channels.keySet());
        // mark 返回找到的通道
        return channel;
    }


    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Return the channel with the specified id if it was disconnected, but not yet closed
     * since there are outstanding messages to be processed.
     */
    public KafkaChannel closingChannel(String id) {
        return closingChannels.get(id);
    }

    /**
     * Returns the lowest priority channel chosen using the following sequence:
     *   1) If one or more channels are in closing state, return any one of them
     *   2) If idle expiry manager is enabled, return the least recently updated channel
     *   3) Otherwise return any of the channels
     *
     * This method is used to close a channel to accommodate a new channel on the inter-broker listener
     * when broker-wide `max.connections` limit is enabled.
     */
    public KafkaChannel lowestPriorityChannel() {
        KafkaChannel channel = null;
        if (!closingChannels.isEmpty()) {
            channel = closingChannels.values().iterator().next();
        } else if (idleExpiryManager != null && !idleExpiryManager.lruConnections.isEmpty()) {
            String channelId = idleExpiryManager.lruConnections.keySet().iterator().next();
            channel = channel(channelId);
        } else if (!channels.isEmpty()) {
            channel = channels.values().iterator().next();
        }
        return channel;
    }

    /**
     * 获取与 selectionKey 关联的通道
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a completed receive
     */
    private boolean hasCompletedReceive(KafkaChannel channel) {
        return completedReceives.containsKey(channel.id());
    }

    /**
     * adds a receive to completed receives
     */
    private void addToCompletedReceives(KafkaChannel channel, NetworkReceive networkReceive, long currentTimeMs) {
        // mark 检查是否还有当前通道未进行处理的接收对象
        if (hasCompletedReceive(channel))
            throw new IllegalStateException("Attempting to add second completed receive to channel " + channel.id());

        // mark 添加到完成接收队列中进行等待
        this.completedReceives.put(channel.id(), networkReceive);
        // mark 记录接收的消息大小和接收时间戳
        sensors.recordCompletedReceive(channel.id(), networkReceive.size(), currentTimeMs);
    }

    // only for testing
    public Set<SelectionKey> keys() {
        return new HashSet<>(nioSelector.keys());
    }


    class SelectorChannelMetadataRegistry implements ChannelMetadataRegistry {
        // mark 通道SSL使用的加密套件
        private CipherInformation cipherInformation;
        // mark 通道绑定的客户端信息
        private ClientInformation clientInformation;

        @Override
        public void registerCipherInformation(final CipherInformation cipherInformation) {
            if (this.cipherInformation != null) {
                if (this.cipherInformation.equals(cipherInformation))
                    return;
                sensors.connectionsByCipher.decrement(this.cipherInformation);
            }

            this.cipherInformation = cipherInformation;
            sensors.connectionsByCipher.increment(cipherInformation);
        }

        @Override
        public CipherInformation cipherInformation() {
            return cipherInformation;
        }

        @Override
        public void registerClientInformation(final ClientInformation clientInformation) {
            if (this.clientInformation != null) {
                if (this.clientInformation.equals(clientInformation))
                    return;
                sensors.connectionsByClient.decrement(this.clientInformation);
            }

            this.clientInformation = clientInformation;
            sensors.connectionsByClient.increment(clientInformation);
        }

        @Override
        public ClientInformation clientInformation() {
            return clientInformation;
        }

        @Override
        public void close() {
            if (this.cipherInformation != null) {
                sensors.connectionsByCipher.decrement(this.cipherInformation);
                this.cipherInformation = null;
            }

            if (this.clientInformation != null) {
                sensors.connectionsByClient.decrement(this.clientInformation);
                this.clientInformation = null;
            }
        }
    }

    class SelectorMetrics implements AutoCloseable {
        private final Metrics metrics;
        private final Map<String, String> metricTags;
        private final boolean metricsPerConnection;
        private final String metricGrpName;
        private final String perConnectionMetricGrpName;

        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor successfulAuthentication;
        public final Sensor successfulReauthentication;
        public final Sensor successfulAuthenticationNoReauth;
        public final Sensor reauthenticationLatency;
        public final Sensor failedAuthentication;
        public final Sensor failedReauthentication;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor requestsSent;
        public final Sensor bytesReceived;
        public final Sensor responsesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;
        public final IntGaugeSuite<CipherInformation> connectionsByCipher;
        public final IntGaugeSuite<ClientInformation> connectionsByClient;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection) {
            this.metrics = metrics;
            this.metricTags = metricTags;
            this.metricsPerConnection = metricsPerConnection;
            this.metricGrpName = metricGrpPrefix + "-metrics";
            this.perConnectionMetricGrpName = metricGrpPrefix + "-node-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix);
            this.connectionClosed.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-close", "connections closed"));

            this.connectionCreated = sensor("connections-created:" + tagsSuffix);
            this.connectionCreated.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-creation", "new connections established"));

            this.successfulAuthentication = sensor("successful-authentication:" + tagsSuffix);
            this.successfulAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-authentication", "connections with successful authentication"));

            this.successfulReauthentication = sensor("successful-reauthentication:" + tagsSuffix);
            this.successfulReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-reauthentication", "successful re-authentication of connections"));

            this.successfulAuthenticationNoReauth = sensor("successful-authentication-no-reauth:" + tagsSuffix);
            MetricName successfulAuthenticationNoReauthMetricName = metrics.metricName(
                    "successful-authentication-no-reauth-total", metricGrpName,
                    "The total number of connections with successful authentication where the client does not support re-authentication",
                    metricTags);
            this.successfulAuthenticationNoReauth.add(successfulAuthenticationNoReauthMetricName, new CumulativeSum());

            this.failedAuthentication = sensor("failed-authentication:" + tagsSuffix);
            this.failedAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-authentication", "connections with failed authentication"));

            this.failedReauthentication = sensor("failed-reauthentication:" + tagsSuffix);
            this.failedReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-reauthentication", "failed re-authentication of connections"));

            this.reauthenticationLatency = sensor("reauthentication-latency:" + tagsSuffix);
            MetricName reauthenticationLatencyMaxMetricName = metrics.metricName("reauthentication-latency-max",
                    metricGrpName, "The max latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyMaxMetricName, new Max());
            MetricName reauthenticationLatencyAvgMetricName = metrics.metricName("reauthentication-latency-avg",
                    metricGrpName, "The average latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyAvgMetricName, new Avg());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix);
            bytesTransferred.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "network-io", "network operations (reads or writes) on all connections"));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix, bytesTransferred);
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags,
                    "outgoing-byte", "outgoing bytes sent to all servers"));

            this.requestsSent = sensor("requests-sent:" + tagsSuffix);
            this.requestsSent.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "request", "requests sent"));
            MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", metricTags);
            this.requestsSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", metricTags);
            this.requestsSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix, bytesTransferred);
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    "incoming-byte", "bytes read off all sockets"));

            this.responsesReceived = sensor("responses-received:" + tagsSuffix);
            this.responsesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "response", "responses received"));

            this.selectTime = sensor("select-time:" + tagsSuffix);
            this.selectTime.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "select", "times the I/O layer checked for new I/O to perform"));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            this.selectTime.add(createIOThreadRatioMeterLegacy(metrics, metricGrpName, metricTags, "io-wait", "waiting"));
            this.selectTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io-wait", "waiting"));

            this.ioTime = sensor("io-time:" + tagsSuffix);
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            this.ioTime.add(createIOThreadRatioMeterLegacy(metrics, metricGrpName, metricTags, "io", "doing I/O"));
            this.ioTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io", "doing I/O"));

            this.connectionsByCipher = new IntGaugeSuite<>(log, "sslCiphers", metrics,
                cipherInformation -> {
                    Map<String, String> tags = new LinkedHashMap<>();
                    tags.put("cipher", cipherInformation.cipher());
                    tags.put("protocol", cipherInformation.protocol());
                    tags.putAll(metricTags);
                    return metrics.metricName("connections", metricGrpName, "The number of connections with this SSL cipher and protocol.", tags);
                }, 100);

            this.connectionsByClient = new IntGaugeSuite<>(log, "clients", metrics,
                clientInformation -> {
                    Map<String, String> tags = new LinkedHashMap<>();
                    tags.put("clientSoftwareName", clientInformation.softwareName());
                    tags.put("clientSoftwareVersion", clientInformation.softwareVersion());
                    tags.putAll(metricTags);
                    return metrics.metricName("connections", metricGrpName, "The number of connections with this client and version.", tags);
                }, 100);

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, (config, now) -> channels.size());
        }

        private Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
                SampledStat stat, String baseName, String descriptiveName) {
            MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
                            String.format("The number of %s per second", descriptiveName), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
                            String.format("The total number of %s", descriptiveName), metricTags);
            if (stat == null)
                return new Meter(rateMetricName, totalMetricName);
            else
                return new Meter(stat, rateMetricName, totalMetricName);
        }

        private Meter createMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String descriptiveName) {
            return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName);
        }

        /**
         * This method generates `time-total` metrics but has a couple of deficiencies: no `-ns` suffix and no dash between basename
         * and `time-toal` suffix.
         * @deprecated use {{@link #createIOThreadRatioMeter(Metrics, String, Map, String, String)}} for new metrics instead
         */
        @Deprecated
        private Meter createIOThreadRatioMeterLegacy(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String action) {
            MetricName rateMetricName = metrics.metricName(baseName + "-ratio", groupName,
                    String.format("*Deprecated* The fraction of time the I/O thread spent %s", action), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "time-total", groupName,
                    String.format("*Deprecated* The total time the I/O thread spent %s", action), metricTags);
            return new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName);
        }

        private Meter createIOThreadRatioMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                                               String baseName, String action) {
            MetricName rateMetricName = metrics.metricName(baseName + "-ratio", groupName,
                String.format("The fraction of time the I/O thread spent %s", action), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-time-ns-total", groupName,
                String.format("The total time the I/O thread spent %s", action), metricTags);
            return new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName);
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    nodeRequest.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "request", "requests sent"));
                    MetricName metricName = metrics.metricName("request-size-avg", perConnectionMetricGrpName, "The average size of requests sent.", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", perConnectionMetricGrpName, "The maximum size of any request sent.", tags);
                    nodeRequest.add(metricName, new Max());

                    String bytesSentName = "node-" + connectionId + ".bytes-sent";
                    Sensor bytesSent = sensor(bytesSentName);
                    bytesSent.add(createMeter(metrics, perConnectionMetricGrpName, tags, "outgoing-byte", "outgoing bytes"));

                    String nodeResponseName = "node-" + connectionId + ".responses-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    nodeResponse.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "response", "responses received"));

                    String bytesReceivedName = "node-" + connectionId + ".bytes-received";
                    Sensor bytesReceive = sensor(bytesReceivedName);
                    bytesReceive.add(createMeter(metrics, perConnectionMetricGrpName, tags, "incoming-byte", "incoming bytes"));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", perConnectionMetricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", perConnectionMetricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes, long currentTimeMs) {
            this.bytesSent.record(bytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String bytesSentName = "node-" + connectionId + ".bytes-sent";
                Sensor bytesSent = this.metrics.getSensor(bytesSentName);
                if (bytesSent != null)
                    bytesSent.record(bytes, currentTimeMs);
            }
        }

        public void recordCompletedSend(String connectionId, long totalBytes, long currentTimeMs) {
            requestsSent.record(totalBytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(totalBytes, currentTimeMs);
            }
        }

        public void recordBytesReceived(String connectionId, long bytes, long currentTimeMs) {
            this.bytesReceived.record(bytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String bytesReceivedName = "node-" + connectionId + ".bytes-received";
                Sensor bytesReceived = this.metrics.getSensor(bytesReceivedName);
                if (bytesReceived != null)
                    bytesReceived.record(bytes, currentTimeMs);
            }
        }

        public void recordCompletedReceive(String connectionId, long totalBytes, long currentTimeMs) {
            responsesReceived.record(totalBytes, currentTimeMs, false);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".responses-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(totalBytes, currentTimeMs);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
            connectionsByCipher.close();
            connectionsByClient.close();
        }
    }

    /**
     * 封装一个因认证失败而在经过特定延迟后必须关闭的通道。
     */
    private class DelayedAuthenticationFailureClose {
        private final KafkaChannel channel;
        private final long endTimeNanos;
        private boolean closed;

        /**
         * @param channel The channel whose close is being delayed
         * @param delayMs The amount of time by which the operation should be delayed
         */
        public DelayedAuthenticationFailureClose(KafkaChannel channel, int delayMs) {
            this.channel = channel;
            this.endTimeNanos = time.nanoseconds() + (delayMs * 1000L * 1000L);
            this.closed = false;
        }

        /**
         * Try to close this channel if the delay has expired.
         * @param currentTimeNanos The current time
         * @return True if the delay has expired and the channel was closed; false otherwise
         */
        public final boolean tryClose(long currentTimeNanos) {
            if (endTimeNanos <= currentTimeNanos)
                closeNow();
            return closed;
        }

        /**
         * Close the channel now, regardless of whether the delay has expired or not.
         */
        public final void closeNow() {
            if (closed)
                throw new IllegalStateException("Attempt to close a channel that has already been closed");
            handleCloseOnAuthenticationFailure(channel);
            closed = true;
        }
    }

    // mark 用于跟踪最近最少使用的连接以启用空闲连接关闭的帮助程序类
    private static class IdleExpiryManager {
        private final Map<String, Long> lruConnections;
        private final long connectionsMaxIdleNanos;
        private long nextIdleCloseCheckTime;

        /**
         * 构造一个IdleExpiryManager对象。
         *
         * @param time 时间管理接口，用于获取当前时间并执行与时间相关的操作。
         * @param connectionsMaxIdleMs 连接最大空闲时间（毫秒），用于确定何时应认为连接处于空闲状态并关闭。
         */
        public IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            // mark 将连接的最大空闲时间从毫秒转换为纳秒，以便进行更精确的时间控制。
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;

            //mark  初始容量和加载因子采用默认值，我们显式设置它们是因为我们希望设置accessOrder = true，
            // 这意味着LinkedHashMap将按照访问顺序维护元素的顺序。
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);

            // mark 计算下一次检查空闲连接关闭的时间点。
            this.nextIdleCloseCheckTime = time.nanoseconds() + this.connectionsMaxIdleNanos;
        }


        public void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        public Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            // mark 时间还没到 返回null
            if (currentTimeNanos <= nextIdleCloseCheckTime)
                return null;

            // mark 时间到了 但是连接列表为空 则重置下次检查点 并返回null
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
                return null;
            }

            // mark 取出最早的连接
            Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();

            // mark 检查连接是否已经超过最大空闲时间

            Long connectionLastActiveTime = oldestConnectionEntry.getValue();
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;

            if (currentTimeNanos > nextIdleCloseCheckTime)
                return oldestConnectionEntry;
            else
                return null;
        }

        public void remove(String connectionId) {
            lruConnections.remove(connectionId);
        }
    }

    //package-private for testing
    boolean isOutOfMemory() {
        return outOfMemory;
    }

    //package-private for testing
    boolean isMadeReadProgressLastPoll() {
        return madeReadProgressLastPoll;
    }

    // package-private for testing
    Map<?, ?> delayedClosingChannels() {
        return delayedClosingChannels;
    }
}
