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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 一个 Kafka 连接可以是客户端上现有的（可能是一个代理服务器，在代理间场景中）并表示到远程代理的通道，
 * 或反之（存在于代理服务器上，表示到远程客户端的通道，在代理间场景中客户端也可能是代理服务器）。
 * <p>
 * 每个实例具有以下内容：
 * <ul>
 * <li>一个唯一的 ID，用于在客户端侧的 {@code KafkaClient} 实例中识别该连接，或者在服务器端接收的实例中识别该连接</li>
 * <li>一个对底层 {@link TransportLayer} 的引用，用于允许读写操作</li>
 * <li>一个 {@link Authenticator}，它通过直接从/向同一个 {@link TransportLayer} 读取和写入来执行身份验证（或重新身份验证，
 * 如果该功能已启用并且适用于此连接）</li>
 * <li>一个 {@link MemoryPool}，用于读取响应（通常对于客户端是 JVM 堆，虽然可以为代理服务器和测试内存不足场景使用较小的池）</li>
 * <li>一个 {@link NetworkReceive}，表示当前未完成/进行中的请求（从服务器端的角度）或响应（从客户端的角度），
 * 如果适用；或一个尚未读取任何数据的非空值，或者如果没有进行中的请求/响应则为 null</li>
 * <li>一个 {@link Send}，表示当前的请求（从客户端的角度）或响应（从服务器端的角度），可能是等待发送或部分发送，
 * 如果适用，或者为 null</li>
 * <li>一个 {@link ChannelMuteState}，记录通道是否由于内存压力或其他原因被静音</li>
 * </ul>
 */
public class KafkaChannel implements AutoCloseable {
    private static final long MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS = 1000 * 1000 * 1000;

    /**
     * Mute States for KafkaChannel:
     * <ul>
     *   <li> NOT_MUTED: Channel is not muted. This is the default state. </li>
     *   <li> MUTED: Channel is muted. Channel must be in this state to be unmuted. </li>
     *   <li> MUTED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted and SocketServer has not sent a response
     *                                    back to the client yet (acks != 0) or is currently waiting to receive a
     *                                    response from the API layer (acks == 0). </li>
     *   <li> MUTED_AND_THROTTLED: (SocketServer only) Channel is muted and throttling is in progress due to quota
     *                             violation. </li>
     *   <li> MUTED_AND_THROTTLED_AND_RESPONSE_PENDING: (SocketServer only) Channel is muted, throttling is in progress,
     *                                                  and a response is currently pending. </li>
     * </ul>
     */
    public enum ChannelMuteState {
        NOT_MUTED,
        MUTED,
        MUTED_AND_RESPONSE_PENDING,
        MUTED_AND_THROTTLED,
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    }

    /** Socket server events that will change the mute state:
     * <ul>
     *   <li> REQUEST_RECEIVED: A request has been received from the client. </li>
     *   <li> RESPONSE_SENT: A response has been sent out to the client (ack != 0) or SocketServer has heard back from
     *                       the API layer (acks = 0) </li>
     *   <li> THROTTLE_STARTED: Throttling started due to quota violation. </li>
     *   <li> THROTTLE_ENDED: Throttling ended. </li>
     * </ul>
     *
     * Valid transitions on each event are:
     * <ul>
     *   <li> REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING </li>
     *   <li> RESPONSE_SENT:    MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED </li>
     *   <li> THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING </li>
     *   <li> THROTTLE_ENDED:   MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_RESPONSE_PENDING </li>
     * </ul>
     */
    public enum ChannelMuteEvent {
        REQUEST_RECEIVED,
        RESPONSE_SENT,
        THROTTLE_STARTED,
        THROTTLE_ENDED
    }

    private final String id;
    // mark 数据传输层
    private final TransportLayer transportLayer;
    // mark 认证器生成器
    private final Supplier<Authenticator> authenticatorCreator;
    // mark 认证器
    private Authenticator authenticator;

    // Tracks accumulated network thread time. This is updated on the network thread.
    // The values are read and reset after each response is sent.

    private long networkThreadTimeNanos;
    // mark 最大接收大小
    private final int maxReceiveSize;
    // mark 内存池
    private final MemoryPool memoryPool;
    private final ChannelMetadataRegistry metadataRegistry;
    // mark 通道挂载的网络接收数据
    private NetworkReceive receive;
    // mark 通道挂载的网络发送数据包
    private NetworkSend send;
    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private boolean disconnected;
    private ChannelMuteState muteState;
    private ChannelState state;
    private SocketAddress remoteAddress;
    private int successfulAuthentications;
    private boolean midWrite;
    private long lastReauthenticationStartNanos;

    public KafkaChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorCreator,
                        int maxReceiveSize, MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticatorCreator = authenticatorCreator;
        this.authenticator = authenticatorCreator.get();
        this.networkThreadTimeNanos = 0L;
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = memoryPool;
        this.metadataRegistry = metadataRegistry;
        this.disconnected = false;
        this.muteState = ChannelMuteState.NOT_MUTED;
        this.state = ChannelState.NOT_CONNECTED;
    }

    public void close() throws IOException {
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticator, receive, metadataRegistry);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public KafkaPrincipal principal() {
        return authenticator.principal();
    }

    public Optional<KafkaPrincipalSerde> principalSerde() {
        return authenticator.principalSerde();
    }

    /**
     * 使用配置的认证器执行传输层的握手和认证。
     *
     * 对于启用客户端认证的 SSL，{@link TransportLayer#handshake()} 执行认证。
     * 对于 SASL，认证由 {@link Authenticator#authenticate()} 执行。
     */
    public void prepare() throws AuthenticationException, IOException {
        boolean authenticating = false;
        try {
            // mark 如果传输层没有准备好 （主要正对SSL传输层 明文传输层默认是true）
            if (!transportLayer.ready())
                // mark 传输层执行握手过程
                transportLayer.handshake();
            if (transportLayer.ready() && !authenticator.complete()) {
                authenticating = true;
                // mark 调用认证器的 authenticate 进行身份认证
                authenticator.authenticate();
            }
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            if (authenticating) {
                delayCloseOnAuthenticationFailure();
                throw new DelayedResponseAuthenticationException(e);
            }
            throw e;
        }

        if (ready()) {
            ++successfulAuthentications;
            state = ChannelState.READY;
        }
    }

    public void disconnect() {
        disconnected = true;
        if (state == ChannelState.NOT_CONNECTED && remoteAddress != null) {
            //if we captured the remote address we can provide more information
            state = new ChannelState(ChannelState.State.NOT_CONNECTED, remoteAddress.toString());
        }
        transportLayer.disconnect();
    }

    public void state(ChannelState state) {
        this.state = state;
    }

    public ChannelState state() {
        return this.state;
    }

    /**
     * 尝试完成连接。
     * <p>
     * 此方法尝试完成连接的建立。在调用finishConnect()之前，
     * 我们需要获取远程地址，否则如果连接被拒绝，将无法访问远程地址。
     *
     * @return boolean 连接是否成功完成
     * @throws IOException 如果发生I/O错误
     */
    public boolean finishConnect() throws IOException {
        // mark 从传输层中获取socket channel
        SocketChannel socketChannel = transportLayer.socketChannel();

        if (socketChannel != null) {
            remoteAddress = socketChannel.getRemoteAddress();
        }

        // mark 调用传输层结束连接操作 （不再关注连接事件 关注可读事件）
        boolean connected = transportLayer.finishConnect();

        if (connected) {
            // mark 如果传输层和认证层都OK则 kafka channel状态设置为ready
            if (ready()) {
                state = ChannelState.READY;
                // mark 连接状态都设置为 AUTHENTICATE
            } else if (remoteAddress != null) {
                state = new ChannelState(ChannelState.State.AUTHENTICATE, remoteAddress.toString());
            } else {
                state = ChannelState.AUTHENTICATE;
            }
        }

        // 返回连接结果
        return connected;
    }


    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * externally muting a channel should be done via selector to ensure proper state handling
     */
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.MUTED;
        }
    }

    /**
     * 取消静音通道。只有在通道处于 MUTED 状态时才能取消静音。
     * 对于其他静音状态（MUTED_AND_*），这是一个无操作。
     *
     * @return 调用后通道是否处于 NOT_MUTED 状态
     */
    boolean maybeUnmute() {
        // mark 如果通道状态是静默的则传输层增加可读时间监听
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    // Handle the specified channel mute-related event and transition the mute state according to the state machine.
    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    public ChannelMuteState muteState() {
        return muteState;
    }

    /**
     * Delay channel close on authentication failure. This will remove all read/write operations from the channel until
     * {@link #completeCloseOnAuthenticationFailure()} is called to finish up the channel close.
     */
    private void delayCloseOnAuthenticationFailure() {
        transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * Finish up any processing on {@link #prepare()} failure.
     * @throws IOException
     */
    void completeCloseOnAuthenticationFailure() throws IOException {
        transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        // Invoke the underlying handler to finish up any processing on authentication failure
        authenticator.handleAuthenticationFailure();
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMuted() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    public boolean isInMutableState() {
        //some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
        //receive.memoryAllocated() is expected to return true)
        if (receive == null || receive.memoryAllocated())
            return false;
        //also cannot mute if underlying transport is not in the ready state
        return transportLayer.ready();
    }

    /**
     * 检查kafka通道是否准备完毕
     * <p>
     * 该方法通过检查两个条件来确定对象是否准备好：
     * <ul>
     *   <li>{@link #transportLayer} 的 {@code ready()} 方法是否返回 {@code true}</li>
     *   <li>{@link #authenticator} 的 {@code complete()} 方法是否返回 {@code true}</li>
     * </ul>
     * 如果这两个条件都满足，则返回 {@code true}，表示对象已准备好；否则返回 {@code false}。
     * </p>
     *
     * @return 如果 {@link #transportLayer} 和 {@link #authenticator} 都表示准备好，则返回 {@code true}；
     * 否则返回 {@code false}。
     */
    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 将发送的数据挂载到kafka channel中
     *
     * 此方法用于设定当前连接的发送处理器。在设定之前，会检查是否已经有其他的发送处理器正在使用，
     * 如果存在，则抛出IllegalStateException，表示不能在前一个发送操作尚未完成的情况下开始新的发送操作。
     * 这种设计保证了每个连接在任意时刻只进行一个发送操作，避免了并发场景下的发送冲突。
     *
     * @param send 发送处理器，用于处理网络发送操作。
     * @throws IllegalStateException 如果已有发送操作正在进行，则抛出此异常。
     */
    public void setSend(NetworkSend send) {
        // 检查是否已经有发送处理器，如果存在则抛出异常
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
        // 设置新的发送处理器
        // mark 挂载send
        this.send = send;
        // 对应的传输层添加写兴趣操作，表示现在可以进行发送操作
        // mark 请求可写事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 尝试完成当前的发送操作
     * 如果当前有发送操作并且已经完成，则将其标记为完成状态，并返回该发送对象
     * 否则，返回null
     *
     * @return 完成的发送对象，如果当前没有发送操作或发送操作未完成，则返回null
     */
    public NetworkSend maybeCompleteSend() {

        /**
         * 这里调用Send对象的completed方法进行确认
         * 具体Send完成发送的逻辑如下：
         *     ByteBufferSend {@link ByteBufferSend#completed()}
         *          1.总体剩余容量小于0
         *          2.发送过程中没有发生pending
         *     RecordsSend {@link org.apache.kafka.common.record.RecordsSend#completed()}
         *          1.总体剩余容量小于0
         *          2.发送过程中没有发生pending
         *     MultiRecordsSend {@link MultiRecordsSend#completed()}
         *          1.当前发送指针为null
         */
        if (send != null && send.completed()) {
            // mark 修改发送中标志 表示发送已经结束
            midWrite = false;
            /**
             * mark 发送结束 通道不再关注可读事件
             * 通道监听可写事件的时机是Send对象被挂载到kafka channel上的时候 详见
             * {@link KafkaChannel#setSend(NetworkSend)} 以及 {@link  Selector#send(NetworkSend)}
             */
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            // mark 清空挂载的send对象并返回结果
            NetworkSend result = send;
            send = null;
            return result;
        }
        return null;
    }

    /**
     * 尝试从网络读取数据，并在必要时分配内存。
     * @return 返回接收到的字节数。
     * @throws IOException 如果在接收操作期间发生I/O错误。
     */
    public long read() throws IOException {
        // mark 检查receive对象是否已初始化，如果没有，进行初始化。
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }

        // mark 尝试接收数据，假设receive方法处理了从网络接收数据的所有细节。
        long bytesReceived = receive(this.receive);

        // mark 如果receive不为空 但是buffer为null说明分配内存的时候失败了
        // mark 此时检查通道是否满足静音条件
        // mark 1. receive==null 或者 receive.buffer！=null 这两种情况下都不足以说明内存分配失败 不满足静音条件
        // mark 2. 传输层通道是否ready 如果ready则可以确认是内存分配失败 则进行静音
        if (this.receive.requiredMemoryAmountKnown() && !this.receive.memoryAllocated() && isInMutableState()) {
            // mark 内存池可能已经耗尽，自我静音。
            mute();
        }
        // mark 返回接收到的字节数。
        return bytesReceived;
    }


    public NetworkReceive currentReceive() {
        return receive;
    }

    /**
     * 尝试完成网络接收操作。
     * <p>
     * 此方法用于检查当前是否有一个已完成的网络接收操作（NetworkReceive）。如果存在，并且该操作可以被完成（complete方法返回true），则该方法会重置这个接收操作的负载（payload）的读取位置，并将当前的接收对象设置为null，以准备接收新的网络数据。
     * <p>
     * 方法返回的是一个可能已完成的网络接收对象，如果当前没有可完成的接收操作，则返回null。
     *
     * @return 如果存在并已完成一个网络接收操作，则返回该操作对象；否则返回null。
     */
    public NetworkReceive maybeCompleteReceive() {
        // mark 如果 receive不为空，且receive已经完成，则将receive设置为null，并返回receive对象。
        // mark 1.receive 的size（4字节）没有空余空间 说明消息头size接收完毕
        // mark 2.receive 的buffer (根据size分配的空间) 没有空余空间 说明payload接收完毕
        if (receive != null && receive.complete()) {
            // mark 重置buffer写指针
            receive.payload().rewind();
            // mark 重置通道的receive并返回当前receive
            NetworkReceive result = receive;
            receive = null;
            return result;
        }
        return null;
    }

    /**
     * 尝试写入数据到传输层
     *
     * @return 写入的数据量如果send为null，则返回0
     * @throws IOException 如果写入过程中发生I/O错误
     */
    public long write() throws IOException {
        if (send == null)
            return 0;
        // mark 添加正在写入标志
        midWrite = true;
        return send.writeTo(transportLayer);
    }

    /**
     * Accumulates network thread time for this channel.
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    /**
     * Returns accumulated network thread time for this channel and resets
     * the value to zero.
     */
    public long getAndResetNetworkThreadTimeNanos() {
        long current = networkThreadTimeNanos;
        networkThreadTimeNanos = 0;
        return current;
    }

    private long receive(NetworkReceive receive) throws IOException {
        try {
            // mark 从传输层接收数据
            return receive.readFrom(transportLayer);
            // mark 如果出现SslAuthenticationException则将空岛状态标记为认证失败
        } catch (SslAuthenticationException e) {
            // With TLSv1.3, post-handshake messages may throw SSLExceptions, which are
            // handled as authentication failures
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            throw e;
        }
    }

    /**
     * @return true if underlying transport has bytes remaining to be read from any underlying intermediate buffers.
     */
    public boolean hasBytesBuffered() {
        return transportLayer.hasBytesBuffered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaChannel that = (KafkaChannel) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return super.toString() + " id=" + id;
    }
    
    /**
     * Return the number of times this instance has successfully authenticated. This
     * value can only exceed 1 when re-authentication is enabled and it has
     * succeeded at least once.
     * 
     * @return the number of times this instance has successfully authenticated
     */
    public int successfulAuthentications() {
        return successfulAuthentications;
    }

    /**
     * If this is a server-side connection that has an expiration time and at least
     * 1 second has passed since the prior re-authentication (if any) started then
     * begin the process of re-authenticating the connection and return true,
     * otherwise return false
     * 
     * @param saslHandshakeNetworkReceive
     *            the mandatory {@link NetworkReceive} containing the
     *            {@code SaslHandshakeRequest} that has been received on the server
     *            and that initiates re-authentication.
     * @param nowNanosSupplier
     *            {@code Supplier} of the current time. The value must be in
     *            nanoseconds as per {@code System.nanoTime()} and is therefore only
     *            useful when compared to such a value -- it's absolute value is
     *            meaningless.
     * 
     * @return true if this is a server-side connection that has an expiration time
     *         and at least 1 second has passed since the prior re-authentication
     *         (if any) started to indicate that the re-authentication process has
     *         begun, otherwise false
     * @throws AuthenticationException
     *             if re-authentication fails due to invalid credentials or other
     *             security configuration errors
     * @throws IOException
     *             if read/write fails due to an I/O error
     * @throws IllegalStateException
     *             if this channel is not "ready"
     */
    public boolean maybeBeginServerReauthentication(NetworkReceive saslHandshakeNetworkReceive,
            Supplier<Long> nowNanosSupplier) throws AuthenticationException, IOException {
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should be \"ready\" when processing SASL Handshake for potential re-authentication");
        /*
         * Re-authentication is disabled if there is no session expiration time, in
         * which case the SASL handshake network receive will be processed normally,
         * which results in a failure result being sent to the client. Also, no need to
         * check if we are muted since we are processing a received packet when we invoke
         * this.
         */
        if (authenticator.serverSessionExpirationTimeNanos() == null)
            return false;
        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        long nowNanos = nowNanosSupplier.get();
        /*
         * Cannot re-authenticate more than once every second; an attempt to do so will
         * result in the SASL handshake network receive being processed normally, which
         * results in a failure result being sent to the client.
         */
        if (lastReauthenticationStartNanos != 0
                && nowNanos - lastReauthenticationStartNanos < MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS)
            return false;
        lastReauthenticationStartNanos = nowNanos;
        swapAuthenticatorsAndBeginReauthentication(
                new ReauthenticationContext(authenticator, saslHandshakeNetworkReceive, nowNanos));
        return true;
    }

    /**
     * 如果这是一个未静音的客户端连接，且没有进行中的写操作，并且定义了已过期的会话过期时间，
     * 则开始重新认证连接的过程并返回 true，否则返回 false。
     *
     * @param nowNanosSupplier
     *            当前时间的 {@code Supplier}。该值必须以纳秒为单位，
     *            如 {@code System.nanoTime()}，因此只有在与该值进行比较时才有意义 —— 它的绝对值是没有意义的。
     *
     * @return 如果这是一个未静音的客户端连接，没有进行中的写操作，并且定义了已过期的会话过期时间，
     *         表示重新认证过程已经开始，则返回 true，否则返回 false。
     *
     * @throws AuthenticationException 如果由于无效的凭据或其他安全配置错误而导致重新认证失败
     * @throws IOException 如果由于 I/O 错误导致读/写失败
     * @throws IllegalStateException 如果此通道未“就绪”
     */
    public boolean maybeBeginClientReauthentication(Supplier<Long> nowNanosSupplier)
            throws AuthenticationException, IOException {
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should always be \"ready\" when it is checked for possible re-authentication");
        // mark 如果认证器重新认证时间为null且通道未静音或者正在写入时 返回false
        if (muteState != ChannelMuteState.NOT_MUTED || midWrite
                || authenticator.clientSessionReauthenticationTimeNanos() == null)
            return false;
        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        // mark 重新认证时间未到 返回fasle
        long nowNanos = nowNanosSupplier.get();
        if (nowNanos < authenticator.clientSessionReauthenticationTimeNanos())
            return false;
        // mark 开始重新认证
        swapAuthenticatorsAndBeginReauthentication(new ReauthenticationContext(authenticator, receive, nowNanos));
        receive = null;
        return true;
    }
    
    /**
     * Return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable, otherwise null.
     * The server-side perspective will yield a lower value than the client-side
     * perspective of the same re-authentication because the client-side observes an
     * additional network round-trip.
     * 
     * @return the number of milliseconds that elapsed while re-authenticating this
     *         session from the perspective of this instance, if applicable,
     *         otherwise null
     */
    public Long reauthenticationLatencyMs() {
        return authenticator.reauthenticationLatencyMs();
    }

    /**
     * Return true if this is a server-side channel and the given time is past the
     * session expiration time, if any, otherwise false
     * 
     * @param nowNanos
     *            the current time in nanoseconds as per {@code System.nanoTime()}
     * @return true if this is a server-side channel and the given time is past the
     *         session expiration time, if any, otherwise false
     */
    public boolean serverAuthenticationSessionExpired(long nowNanos) {
        Long serverSessionExpirationTimeNanos = authenticator.serverSessionExpirationTimeNanos();
        return serverSessionExpirationTimeNanos != null && nowNanos - serverSessionExpirationTimeNanos > 0;
    }
    
    /**
     * Return the (always non-null but possibly empty) client-side
     * {@link NetworkReceive} response that arrived during re-authentication but
     * is unrelated to re-authentication. This corresponds to a request sent
     * prior to the beginning of re-authentication; the request was made when the
     * channel was successfully authenticated, and the response arrived during the
     * re-authentication process.
     * 
     * @return client-side {@link NetworkReceive} response that arrived during
     *         re-authentication that is unrelated to re-authentication. This may
     *         be empty.
     */
    public Optional<NetworkReceive> pollResponseReceivedDuringReauthentication() {
        return authenticator.pollResponseReceivedDuringReauthentication();
    }
    
    /**
     * Return true if this is a server-side channel and the connected client has
     * indicated that it supports re-authentication, otherwise false
     * 
     * @return true if this is a server-side channel and the connected client has
     *         indicated that it supports re-authentication, otherwise false
     */
    boolean connectedClientSupportsReauthentication() {
        return authenticator.connectedClientSupportsReauthentication();
    }

    private void swapAuthenticatorsAndBeginReauthentication(ReauthenticationContext reauthenticationContext)
            throws IOException {
        // it is up to the new authenticator to close the old one
        // replace with a new one and begin the process of re-authenticating
        authenticator = authenticatorCreator.get();
        authenticator.reauthenticate(reauthenticationContext);
    }

    public ChannelMetadataRegistry channelMetadataRegistry() {
        return metadataRegistry;
    }
}
