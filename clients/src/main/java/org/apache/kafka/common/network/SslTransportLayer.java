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

import java.io.IOException;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.CancelledKeyException;

import java.security.Principal;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 * SSL通信的传输层
 * SSL传输层通道的构建可以参考 {@link SslChannelBuilder#buildChannel}
 *
 * TLS v1.3 注释：
 * https://tools.ietf.org/html/rfc8446#section-4.6：握手后消息
 *“TLS 还允许在主握手之后发送其他消息。
 * 这些消息使用握手内容类型并在以下情况下加密
 * 适当的应用程序流量密钥。”
 */
public class SslTransportLayer implements TransportLayer {
    private enum State {
        // 初始状态
        NOT_INITIALIZED,
        // SSLEngine 处于握手模式
        HANDSHAKE,
        // SSL握手失败，连接将被终止
        HANDSHAKE_FAILED,
        // SSLEngine 已完成握手，握手后消息可能正在等待 TLSv1.3
        POST_HANDSHAKE,
        // SSLEngine 已完成握手，任何握手后消息均已针对 TLSv1.3 进行处理
        // 对于 TLSv1.3，握手后处理传入数据时，我们将通道移至 READY 状态
        READY,
        // 通道正在关闭
        CLOSING
    }

    private static final String TLS13 = "TLSv1.3";

    private final String channelId;

    /**
     * SSL引擎的构建过程可以参考 {@link org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createSslEngine}
     */
    private final SSLEngine sslEngine;
    private final SelectionKey key;
    private final SocketChannel socketChannel;
    private final ChannelMetadataRegistry metadataRegistry;
    private final Logger log;

    private HandshakeStatus handshakeStatus;
    private SSLEngineResult handshakeResult;
    private State state;
    private SslAuthenticationException handshakeException;
    private ByteBuffer netReadBuffer;
    private ByteBuffer netWriteBuffer;
    private ByteBuffer appReadBuffer;
    private ByteBuffer fileChannelBuffer;
    private boolean hasBytesBuffered;

    public static SslTransportLayer create(String channelId, SelectionKey key, SSLEngine sslEngine,
                                           ChannelMetadataRegistry metadataRegistry) throws IOException {
        return new SslTransportLayer(channelId, key, sslEngine, metadataRegistry);
    }

    // Prefer `create`, only use this in tests
    SslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine,
                      ChannelMetadataRegistry metadataRegistry) {
        this.channelId = channelId;
        this.key = key;
        this.socketChannel = (SocketChannel) key.channel();
        this.sslEngine = sslEngine;
        this.state = State.NOT_INITIALIZED;
        this.metadataRegistry = metadataRegistry;

        final LogContext logContext = new LogContext(String.format("[SslTransportLayer channelId=%s key=%s] ", channelId, key));
        this.log = logContext.logger(getClass());
    }

    /**
     * 开始SSL/TLS握手过程。
     * 此方法初始化SSL引擎所需的缓冲区并启动握手过程。
     * 它被设计为仅调用一次以确保正确建立SSL连接。
     * 当状态不是NOT_INITIALIZED时再次调用此方法将抛出IllegalStateException。
     *
     * @throws IllegalStateException 如果握手已经发起或SSL引擎处于不适当的状态。
     * @throws IOException           如果在握手过程中发生I/O错误。
     */
    // 为了测试可见
    protected void startHandshake() throws IOException {
        // 确保仅初始化握手一次。
        if (state != State.NOT_INITIALIZED)
            throw new IllegalStateException("startHandshake() can only be called once, state " + state);

        // 初始化网络读写缓冲区以及应用读缓冲区。
        this.netReadBuffer = ByteBuffer.allocate(netReadBufferSize());
        this.netWriteBuffer = ByteBuffer.allocate(netWriteBufferSize());
        this.appReadBuffer = ByteBuffer.allocate(applicationBufferSize());

        // 设置网络写缓冲区的初始限制为0，表示没有数据要写入。
        netWriteBuffer.limit(0);
        netReadBuffer.limit(0);

        // mark 更新SSL传输层状态为HANDSHAKE。
        state = State.HANDSHAKE;
        // mark 启动SSL/TLS握手过程。
        sslEngine.beginHandshake();
        // mark 获取当前握手状态以确定下一步操作。
        handshakeStatus = sslEngine.getHandshakeStatus();
    }


    /**
     * 检查传输层是否准备好。
     * 明文传输无需准备
     * @return 始终返回 {@code true}，指示对象已准备好。
     */
    @Override
    public boolean ready() {
        return state == State.POST_HANDSHAKE || state == State.READY;
    }

    /**
     * does socketChannel.finishConnect()
     */
    @Override
    public boolean finishConnect() throws IOException {
        boolean connected = socketChannel.finishConnect();
        if (connected)
            // mark 取消关注连接事件 并添加关注读事件
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        return connected;
    }

    /**
     * disconnects selectionKey.
     */
    @Override
    public void disconnect() {
        key.cancel();
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    /**
    * Sends an SSL close message and closes socketChannel.
    */
    @Override
    public void close() throws IOException {
        State prevState = state;
        if (state == State.CLOSING) return;
        state = State.CLOSING;
        sslEngine.closeOutbound();
        try {
            if (prevState != State.NOT_INITIALIZED && isConnected()) {
                if (!flush(netWriteBuffer)) {
                    throw new IOException("Remaining data in the network buffer, can't send SSL close message.");
                }
                //prep the buffer for the close message
                netWriteBuffer.clear();
                //perform the close, since we called sslEngine.closeOutbound
                SSLEngineResult wrapResult = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer);
                //we should be in a close state
                if (wrapResult.getStatus() != SSLEngineResult.Status.CLOSED) {
                    throw new IOException("Unexpected status returned by SSLEngine.wrap, expected CLOSED, received " +
                            wrapResult.getStatus() + ". Will not send close message to peer.");
                }
                netWriteBuffer.flip();
                flush(netWriteBuffer);
            }
        } catch (IOException ie) {
            log.debug("Failed to send SSL Close message", ie);
        } finally {
            socketChannel.socket().close();
            socketChannel.close();
            netReadBuffer = null;
            netWriteBuffer = null;
            appReadBuffer = null;
            if (fileChannelBuffer != null) {
                ByteBufferUnmapper.unmap("fileChannelBuffer", fileChannelBuffer);
                fileChannelBuffer = null;
            }
        }
    }

    /**
     * returns true if there are any pending contents in netWriteBuffer
     */
    @Override
    public boolean hasPendingWrites() {
        return netWriteBuffer.hasRemaining();
    }


    /**
     * 从套接字通道读取可用字节到 netReadBuffer。
     * @return 读取的字节数
     */
    protected int readFromSocketChannel() throws IOException {
        return socketChannel.read(netReadBuffer);
    }

    /**
     * 将缓冲区刷新到网络，非阻塞。
     * 仅用于测试可见。
     * @param buf ByteBuffer 缓冲区
     * @return boolean 如果缓冲区已被清空，则返回 true，否则返回 false
     * @throws IOException 如果发生 I/O 错误
     */
    protected boolean flush(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining > 0) {
            int written = socketChannel.write(buf);
            return written >= remaining;
        }
        return true;
    }

    /**
     * 执行 SSL 握手，非阻塞。网上看到的都是阻塞的，所以这里需要自己实现。
     * 在发送应用数据（kafka 协议）之前，客户端和 kafka 代理必须执行 SSL 握手。
     * 在握手过程中，SSLEngine 生成将通过 socketChannel 传输的加密数据。
     * 每次 SSLEngine 操作都会生成 SSLEngineResult，其中的 SSLEngineResult.handshakeStatus 字段
     * 用于确定需要执行什么操作来推进握手。
     * 典型的握手过程可能如下所示。
     * +-------------+----------------------------------+-------------+
     * |  客户端      |  SSL/TLS 消息                    | HSStatus    |
     * +-------------+----------------------------------+-------------+
     * | wrap()      | ClientHello                      | NEED_UNWRAP |
     * | unwrap()    | ServerHello/Cert/ServerHelloDone | NEED_WRAP   |
     * | wrap()      | ClientKeyExchange                | NEED_WRAP   |
     * | wrap()      | ChangeCipherSpec                 | NEED_WRAP   |
     * | wrap()      | Finished                         | NEED_UNWRAP |
     * | unwrap()    | ChangeCipherSpec                 | NEED_UNWRAP |
     * | unwrap()    | Finished                         | FINISHED    |
     * +-------------+----------------------------------+-------------+
     *
     * @throws IOException 如果读/写失败
     * @throws SslAuthenticationException 如果握手失败并出现 {@link SSLException}
     */
    @Override
    public void handshake() throws IOException {
        // mark 如果传输层状态为未初始化 则执行握手连接（这个地方只会执行一次）
        if (state == State.NOT_INITIALIZED) {
            try {
                // mark 发起握手 (调用SSLEngine的beginHandshake)
                startHandshake();
            } catch (SSLException e) {
                maybeProcessHandshakeFailure(e, false, null);
            }
        }
        // mark 如果在握手的过程中 通道可用 那就见鬼了 抛出异常
        if (ready())
            throw renegotiationException();

        if (state == State.CLOSING)
            throw closingException();

        int read = 0;
        boolean readable = key.isReadable();
        try {
            // Read any available bytes before attempting any writes to ensure that handshake failures
            // reported by the peer are processed even if writes fail (since peer closes connection
            // if handshake fails)
            if (readable)
                read = readFromSocketChannel();

            doHandshake();
            if (ready())
                updateBytesBuffered(true);
        } catch (SSLException e)
        {
            maybeProcessHandshakeFailure(e, true, null);
        } catch (IOException e)
        {
            maybeThrowSslAuthenticationException();

            // This exception could be due to a write. If there is data available to unwrap in the buffer, or data available
            // in the socket channel to read and unwrap, process the data so that any SSL handshake exceptions are reported.
            try {
                do {
                    log.trace("Process any available bytes from peer, netReadBuffer {} netWriterBuffer {} handshakeStatus {} readable? {}",
                        netReadBuffer, netWriteBuffer, handshakeStatus, readable);
                    handshakeWrapAfterFailure(false);
                    handshakeUnwrap(false, true);
                } while (readable && readFromSocketChannel() > 0);
            } catch (SSLException e1) {
                maybeProcessHandshakeFailure(e1, false, e);
            }

            // If we get here, this is not a handshake failure, throw the original IOException
            throw e;
        }

        // Read from socket failed, so throw any pending handshake exception or EOF exception.
        if (read == -1) {
            maybeThrowSslAuthenticationException();
            throw new EOFException("EOF during handshake, handshake status is " + handshakeStatus);
        }
    }

    @SuppressWarnings("fallthrough")
    private void doHandshake() throws IOException {
        boolean read = key.isReadable();
        boolean write = key.isWritable();
        handshakeStatus = sslEngine.getHandshakeStatus();
        if (!flush(netWriteBuffer)) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            return;
        }
        // Throw any pending handshake exception since `netWriteBuffer` has been flushed
        maybeThrowSslAuthenticationException();

        switch (handshakeStatus) {

            case NEED_TASK:
                log.trace("SSLHandshake NEED_TASK channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                handshakeStatus = runDelegatedTasks();
                break;

            // mark wrap操作 app buffer -> net buffer socket需要可写事件完备
            case NEED_WRAP:
                log.trace("SSLHandshake NEED_WRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                handshakeResult = handshakeWrap(write);
                // mark wrap阶段如果出现BUFFER_OVERFLOW说明netWriteBuffer空间不足
                if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    int currentNetWriteBufferSize = netWriteBufferSize();
                    // mark 将未读数据往前压缩 保证扩容时不影响未读数据
                    netWriteBuffer.compact();
                    // mark 对netWriteBuffer进行扩容
                    netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, currentNetWriteBufferSize);
                    // mark 切换读模式
                    netWriteBuffer.flip();
                    if (netWriteBuffer.limit() >= currentNetWriteBufferSize) {
                        throw new IllegalStateException("Buffer overflow when available data size (" + netWriteBuffer.limit() +
                                                        ") >= network buffer size (" + currentNetWriteBufferSize + ")");
                    }
                // mark 下面两个状态都是不应该收到
                }
                else if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    throw new IllegalStateException("Should not have received BUFFER_UNDERFLOW during handshake WRAP.");
                }
                else if (handshakeResult.getStatus() == Status.CLOSED) {
                    throw new EOFException();
                }

                log.trace("SSLHandshake NEED_WRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                       channelId, handshakeResult, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                //如果握手状态不是 NEED_UNWRAP 或无法刷新 netWriteBuffer 内容
                //我们将在这里中断，否则我们可以在同一个调用中执行 need_unwrap 。
                if (handshakeStatus != HandshakeStatus.NEED_UNWRAP || !flush(netWriteBuffer)) {
                    // mark 如果不为unwrap说明后续操作需要继续wrap 调整监听事件为可写事件
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    break;
                }
                // mark 这里break 说明如果wrap后跟unwrap操作 可以直接执行向下仅需执行unwrap操作

            // mark unwrap操作 net buffer -> app buffer socket需要可读事件完备
            case NEED_UNWRAP:
                log.trace("SSLHandshake NEED_UNWRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                do {

                    handshakeResult = handshakeUnwrap(read, false);
                    // mark 如果unwrap阶段 出现BUFFER_OVERFLOW 说明app buffer不足 需要扩容
                    if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                        // mark 对app buffer进行扩容
                        int currentAppBufferSize = applicationBufferSize();
                        appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentAppBufferSize);
                        if (appReadBuffer.position() > currentAppBufferSize) {
                            throw new IllegalStateException("Buffer underflow when available data size (" + appReadBuffer.position() +
                                                           ") > packet buffer size (" + currentAppBufferSize + ")");
                        }
                    }
                } while (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW);

                // mark 理论上只会在unwrap阶段出现BUFFER_UNDERFLOW 要么是半包问题 要么是数据读取不完整
                if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    // mark 这里直接进行扩容 如果是半包问题 (netReadBuffer.position() >= currentNetReadBufferSize) 则直接报错
                    int currentNetReadBufferSize = netReadBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
                    if (netReadBuffer.position() >= currentNetReadBufferSize) {
                        throw new IllegalStateException("Buffer underflow when there is available data");
                    }
                } else if (handshakeResult.getStatus() == Status.CLOSED) {
                    throw new EOFException("SSL handshake status CLOSED during handshake UNWRAP");
                }
                log.trace("SSLHandshake NEED_UNWRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, handshakeResult, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());

                // mark 如果握手状态已完成，则直接进入完成状态。
                //握手完成后，socketChannel中没有数据可供读/写。
                //因此，如果我们不在这里完成握手，选择器将不会调用此通道。

                if (handshakeStatus != HandshakeStatus.FINISHED) {
                    // mark 如果是NEED_WRAP状态则需要继续监听可写事件
                    if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    // mark 如果是NEED_UNWRAP状态则需要继续监听可读事件
                    } else if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                    }
                    break;
                }
            case FINISHED:
                handshakeFinished();
                break;
            case NOT_HANDSHAKING:
                handshakeFinished();
                break;
            default:
                throw new IllegalStateException(String.format("Unexpected status [%s]", handshakeStatus));
        }
    }

    private SSLHandshakeException renegotiationException() {
        return new SSLHandshakeException("Renegotiation is not supported");
    }

    private IllegalStateException closingException() {
        throw new IllegalStateException("Channel is in closing state");
    }

    /**
     * Executes the SSLEngine tasks needed.
     * @return HandshakeStatus
     */
    private HandshakeStatus runDelegatedTasks() {
        for (;;) {
            Runnable task = delegatedTask();
            if (task == null) {
                break;
            }
            task.run();
        }
        return sslEngine.getHandshakeStatus();
    }

    /**
     * 检查握手状态是否完成
     * 设置 selectionKey 的 interestOps。
     */
    private void handshakeFinished() throws IOException {
        // SSLEngine.getHandshakeStatus 是暂时的，它不能正确记录 FINISHED 状态。
        // 握手完成后可以从FINISHED状态转为NOT_HANDSHAKING状态。
        // 因此我们还需要检查handshakeResult.getHandshakeStatus()是否握手完成
        if (handshakeResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
            //如果我们已经交付了最后一个数据包，我们就完成了
            //如果完成则删除OP_WRITE，否则仍有数据要写入
            if (netWriteBuffer.hasRemaining())
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            else {
                SSLSession session = sslEngine.getSession();
                // mark 如果协议是TLSv1.3 状态POST_HANDSHAKE 将在握手写入数据时转换成READY
                state = session.getProtocol().equals(TLS13) ? State.POST_HANDSHAKE : State.READY;
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                log.debug("SSL handshake completed successfully with peerHost '{}' peerPort {} peerPrincipal '{}' protocol '{}' cipherSuite '{}'",
                        session.getPeerHost(), session.getPeerPort(), peerPrincipal(), session.getProtocol(), session.getCipherSuite());
                metadataRegistry.registerCipherInformation(
                    new CipherInformation(session.getCipherSuite(),  session.getProtocol()));
            }

            log.trace("SSLHandshake FINISHED channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} ",
                      channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
        } else {
            throw new IOException("NOT_HANDSHAKING during handshake");
        }
    }

    /**
     * 执行 WRAP 功能
     * @param doWrite 布尔值，指示是否进行写操作
     * @return SSLEngineResult
     * @throws IOException 如果发生 I/O 错误
     */
    private SSLEngineResult handshakeWrap(boolean doWrite) throws IOException {
        log.trace("SSLHandshake handshakeWrap {}", channelId);

        if (netWriteBuffer.hasRemaining())
            throw new IllegalStateException("handshakeWrap called with netWriteBuffer not empty");
        //this should never be called with a network buffer that contains data
        //so we can clear it here.
        netWriteBuffer.clear();
        SSLEngineResult result;
        try {
            // mark 将数据wrap到netWriteBuffer
            result = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer);
        } finally {
            //prepare the results to be written
            // mark 切换读模式
            netWriteBuffer.flip();
        }
        // mark 是否需要执行任务
        handshakeStatus = result.getHandshakeStatus();
        if (result.getStatus() == SSLEngineResult.Status.OK &&
            result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
            handshakeStatus = runDelegatedTasks();
        }
        // mark 将数据刷到socket buffer中
        if (doWrite) flush(netWriteBuffer);
        return result;
    }

    /**
     * Perform handshake unwrap
     * @param doRead boolean If true, read more from the socket channel
     * @param ignoreHandshakeStatus If true, continue to unwrap if data available regardless of handshake status
     * @return SSLEngineResult
     * @throws IOException
     */
    private SSLEngineResult handshakeUnwrap(boolean doRead, boolean ignoreHandshakeStatus) throws IOException {
        log.trace("SSLHandshake handshakeUnwrap {}", channelId);
        SSLEngineResult result;
        int read = 0;
        if (doRead)
            // mark 将数据从socket buffer读取到appReadBuffer
            read = readFromSocketChannel();
        boolean cont;

        do {
            //prepare the buffer with the incoming data
            // mark 保存一下position
            int position = netReadBuffer.position();
            // mark 切换读模式
            netReadBuffer.flip();

            // mark 将数据从 netReadBuffer 读取到 appReadBuffer
            result = sslEngine.unwrap(netReadBuffer, appReadBuffer);
            netReadBuffer.compact();
            handshakeStatus = result.getHandshakeStatus();

            // mark 执行附加任务
            if (result.getStatus() == SSLEngineResult.Status.OK &&
                result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
                handshakeStatus = runDelegatedTasks();
            }

            // mark netReadBuffer.position() != position 说明数据还没有读取完需要继续读取
            cont = (result.getStatus() == SSLEngineResult.Status.OK &&
                    handshakeStatus == HandshakeStatus.NEED_UNWRAP) ||
                    (ignoreHandshakeStatus && netReadBuffer.position() != position);

            log.trace("SSLHandshake handshakeUnwrap: handshakeStatus {} status {}", handshakeStatus, result.getStatus());
        } while (netReadBuffer.position() != 0 && cont);

        // Throw EOF exception for failed read after processing already received data
        // so that handshake failures are reported correctly
        if (read == -1)
            throw new EOFException("EOF during handshake, handshake status is " + handshakeStatus);

        return result;
    }


    /**
     * 从该通道读取一系列字节到给定的缓冲区。尽可能多地读取，直到 dst 缓冲区满或者套接字中没有更多数据为止。
     *
     * @param dst 要传输字节的缓冲区
     * @return 读取的字节数，可能为零；如果通道已到达流的末端且没有更多数据可用，则返回 -1
     * @throws IOException 如果发生其他 I/O 错误
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        // mark 如果传输层状态为关闭则返回-1
        if (state == State.CLOSING) return -1;
        // mark 如果传输层未准备完毕则返回0
        else if (!ready()) return 0;

        //if we have unread decrypted data in appReadBuffer read that into dst buffer.
        // mark 将appReadBuffer中的未读数据读入给如的bytebuffer中
        int read = 0;
        if (appReadBuffer.position() > 0) {
            read = readFromAppBuffer(dst);
        }

        boolean readFromNetwork = false;
        boolean isClosed = false;
        // Each loop reads at most once from the socket.
        // mark 如果dst还能装得下
        while (dst.remaining() > 0) {
            int netread = 0;

            // mark 对netReadBuffer先进行扩容
            netReadBuffer = Utils.ensureCapacity(netReadBuffer, netReadBufferSize());

            // mark 将数据读取到netReadBuffer socket receive buffer --> net read buffer
            if (netReadBuffer.remaining() > 0) {
                // mark 将加密内容从socket读取到netReadBuffer
                netread = readFromSocketChannel();
                if (netread > 0)
                    // mark 标记为本次从网络中读取了数据
                    readFromNetwork = true;
            }

            // mark position > 0 说明 netReadBuffer 存在新读取的数据
            while (netReadBuffer.position() > 0) {
                // mark 切换到读模式
                netReadBuffer.flip();

                SSLEngineResult unwrapResult;
                try {
                    // mark 使用ssl engine 将netReadBuffer的加密内容解密到appReadBuffer
                    // mark net read buffer --解密--> app read buffer
                    unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer);

                    if (state == State.POST_HANDSHAKE && appReadBuffer.position() != 0) {
                        // For TLSv1.3, we have finished processing post-handshake messages since we are now processing data
                        // mark TLSv1.3捂手成功后的状态是 POST_HANDSHAKE 首次接收数据才修改为 READY
                        state = State.READY;
                    }
                } catch (SSLException e) {
                    // For TLSv1.3, handle SSL exceptions while processing post-handshake messages as authentication exceptions
                    if (state == State.POST_HANDSHAKE) {
                        // mark 对于TLSv1.3如果握手后首次接收数据失败则表示握手失败
                        state = State.HANDSHAKE_FAILED;
                        throw new SslAuthenticationException("Failed to process post-handshake messages", e);
                    } else
                        throw e;
                }
                // mark 删除 net read buffer 已读的部分
                netReadBuffer.compact();
                // reject renegotiation if TLS < 1.3, key updates for TLS 1.3 are allowed
                // mark TLSv1.3以外的版本不支持重新握手协商
                if (unwrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING &&
                        unwrapResult.getHandshakeStatus() != HandshakeStatus.FINISHED &&
                        unwrapResult.getStatus() == Status.OK &&
                        !sslEngine.getSession().getProtocol().equals(TLS13)) {
                    log.error("Renegotiation requested, but it is not supported, channelId {}, " +
                        "appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} handshakeStatus {}", channelId,
                        appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position(), unwrapResult.getHandshakeStatus());
                    throw renegotiationException();
                }
                // mark 将解密后的内容读取到给定的buffer
                if (unwrapResult.getStatus() == Status.OK) {
                    read += readFromAppBuffer(dst);
                // mark 如果加密结果为BUFFER_OVERFLOW 则需要的app read buffer进行扩容
                } else if (unwrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    int currentApplicationBufferSize = applicationBufferSize();
                    appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentApplicationBufferSize);
                    if (appReadBuffer.position() >= currentApplicationBufferSize) {
                        throw new IllegalStateException("Buffer overflow when available data size (" + appReadBuffer.position() +
                                                        ") >= application buffer size (" + currentApplicationBufferSize + ")");
                    }

                    // appReadBuffer 将扩展到 currentApplicationBufferSize
                    // 我们需要将现有内容读入 dst，然后才能再次展开。如果 dst 中没有空间
                    // 我们可以在这里中断。
                    if (dst.hasRemaining())
                        read += readFromAppBuffer(dst);
                    else
                        break;
                // mark 如果加密结果为BUFFER_UNDERFLOW 则需要的net read buffer进行扩容
                // mark 在unwrap时如果是BUFFER_UNDERFLOW 则说明net read buffer容量不够
                // mark 对net read buffer扩容 没有改变net read buffer的状态 后面可以继续从socket中读取数据
                } else if (unwrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    // mark 扩容 net read buffer
                    int currentNetReadBufferSize = netReadBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
                    if (netReadBuffer.position() >= currentNetReadBufferSize) {
                        throw new IllegalStateException("Buffer underflow when available data size (" + netReadBuffer.position() +
                                                        ") > packet buffer size (" + currentNetReadBufferSize + ")");
                    }
                    break;
                // mark 如果结果为CLOSED 则表示ssl engine关闭
                } else if (unwrapResult.getStatus() == Status.CLOSED) {
                    // If data has been read and unwrapped, return the data. Close will be handled on the next poll.
                    if (appReadBuffer.position() == 0 && read == 0)
                        throw new EOFException();
                    else {
                        isClosed = true;
                        break;
                    }
                }
            }
            if (read == 0 && netread < 0)
                throw new EOFException("EOF during read");
            if (netread <= 0 || isClosed)
                break;
        }
        // mark 如果本次读取有从 socket buffer读取数据 或者 read >0 (从app read buffer中读取 或者两者皆有)
        updateBytesBuffered(readFromNetwork || read > 0);
        // If data has been read and unwrapped, return the data even if end-of-stream, channel will be closed
        // on a subsequent poll.
        return read;
    }


    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }


    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than dsts.length - offset
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if ((offset < 0) || (length < 0) || (offset > dsts.length - length))
            throw new IndexOutOfBoundsException();

        int totalRead = 0;
        int i = offset;
        while (i < length) {
            if (dsts[i].hasRemaining()) {
                int read = read(dsts[i]);
                if (read > 0)
                    totalRead += read;
                else
                    break;
            }
            if (!dsts[i].hasRemaining()) {
                i++;
            }
        }
        return totalRead;
    }


    /**
     * 从给定的缓冲区向此通道写入一系列字节。
     *
     * @param src 要从中检索字节的缓冲区
     * @return 从 src 中读取的字节数，可能为零，如果通道已到达流末尾，则返回 -1
     * @throws IOException 如果发生其他 I/O 错误
     */
    @Override
    public int write(ByteBuffer src) throws IOException {

        if (state == State.CLOSING)
            throw closingException();
        if (!ready())
            return 0;

        int written = 0;

        // mark 如果将 net write buffer的数据刷到socket buffer成功 并且源buffer中还有内容
        while (flush(netWriteBuffer) && src.hasRemaining()) {
            // mark 清空 net write buffer
            netWriteBuffer.clear();

            // mark 使用SSL engin 将 src buffer 中的数据加密到 net write buffer中
            SSLEngineResult wrapResult = sslEngine.wrap(src, netWriteBuffer);

            // mark net write buffer 切换读模式
            netWriteBuffer.flip();


            // mark 如果 TLS < 1.3，则拒绝重新协商，允许 TLS 1.3 的密钥更新
            if (wrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING &&
                    wrapResult.getStatus() == Status.OK &&
                    !sslEngine.getSession().getProtocol().equals(TLS13)) {
                throw renegotiationException();
            }

            // mark 加密成功
            if (wrapResult.getStatus() == Status.OK) {
                written += wrapResult.bytesConsumed();
            // mark 对 net write buffer 进行扩容并重试
            } else if (wrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                // BUFFER_OVERFLOW means that the last `wrap` call had no effect, so we expand the buffer and try again
                netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, netWriteBufferSize());

                netWriteBuffer.position(netWriteBuffer.limit());
            // mark wrap过程中不存在BUFFER_UNDERFLOW 抛出异常
            } else if (wrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                throw new IllegalStateException("SSL BUFFER_UNDERFLOW during write");
            // mark 通道关闭抛出异常
            } else if (wrapResult.getStatus() == Status.CLOSED) {
                throw new EOFException();
            }
        }
        return written;
    }

    /**
     * 从给定缓冲区的子序列向此通道写入一系列字节。
     *
     * @param srcs 要从中检索字节的缓冲区
     * @param offset 缓冲区数组中第一个要检索字节的缓冲区的偏移量；必须是非负数，并且不大于 srcs.length。
     * @param length 要访问的最大缓冲区数量；必须是非负数，并且不大于 srcs.length - offset。
     * @return 返回写入的字节数，可能为零。
     * @throws IOException 如果发生其他 I/O 错误
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        // mark 校验参数
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();

        int totalWritten = 0;
        int i = offset;
        // mark 开始遍历buffer数组
        while (i < length) {
            // mark 如果src缓冲区存在数据 或者 net write buffer中存在未发送数据 则调用write方法
            if (srcs[i].hasRemaining() || hasPendingWrites()) {
                // mark 使用SSL引擎将原buffer中的数据刷入socket buffer
                int written = write(srcs[i]);
                if (written > 0) {
                    totalWritten += written;
                }
            }
            // mark 如果原buffer没有剩余数据 并且 net write buffer没有待发送数据则处理下一个buffer
            if (!srcs[i].hasRemaining() && !hasPendingWrites()) {
                i++;
            } else {
                // mark 这里直接进行了中断
                // mark 如果我们无法将当前缓冲区写入socketChannel，我们应该中断，
                // mark 因为我们可能已经达到最大套接字发送缓冲区大小。
                break;
            }
        }
        return totalWritten;
    }

    /**
     * 从给定的缓冲区向此通道写入一系列字节。
     *
     * @param srcs 要从中检索字节的缓冲区
     * @return 返回被 SSLEngine.wrap 消费的字节数，可能为零。
     * @throws IOException 如果发生其他 I/O 错误
     */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }


    /**
     * SSLSession's peerPrincipal for the remote host.
     * @return Principal
     */
    public Principal peerPrincipal() {
        try {
            return sslEngine.getSession().getPeerPrincipal();
        } catch (SSLPeerUnverifiedException se) {
            log.debug("SSL peer is not authenticated, returning ANONYMOUS instead");
            return KafkaPrincipal.ANONYMOUS;
        }
    }

    /**
     * returns an SSL Session after the handshake is established
     * throws IllegalStateException if the handshake is not established
     */
    public SSLSession sslSession() throws IllegalStateException {
        return sslEngine.getSession();
    }

    /**
     * Adds interestOps to SelectionKey of the TransportLayer
     * @param ops SelectionKey interestOps
     */
    @Override
    public void addInterestOps(int ops) {
        if (!key.isValid())
            throw new CancelledKeyException();
        else if (!ready())
            throw new IllegalStateException("handshake is not completed");

        key.interestOps(key.interestOps() | ops);
    }

    /**
     * removes interestOps to SelectionKey of the TransportLayer
     * @param ops SelectionKey interestOps
     */
    @Override
    public void removeInterestOps(int ops) {
        if (!key.isValid())
            throw new CancelledKeyException();
        else if (!ready())
            throw new IllegalStateException("handshake is not completed");

        key.interestOps(key.interestOps() & ~ops);
    }


    /**
     * returns delegatedTask for the SSLEngine.
     */
    protected Runnable delegatedTask() {
        return sslEngine.getDelegatedTask();
    }

    /**
     * 将 appReadBuffer 的内容（解密后的数据）传输到 dst bytebuffer
     * @param dst ByteBuffer
     */
    private int readFromAppBuffer(ByteBuffer dst) {
        // mark 切换到读模式
        appReadBuffer.flip();
        // mark 计算两个buffer最小的剩余容量
        int remaining = Math.min(appReadBuffer.remaining(), dst.remaining());
        if (remaining > 0) {
            // mark 记录当前的limit
            int limit = appReadBuffer.limit();
            // mark 强制调整limit到dst可以容纳的部分
            appReadBuffer.limit(appReadBuffer.position() + remaining);
            // mark 转移数据
            dst.put(appReadBuffer);
            // mark 将limit调整回去
            appReadBuffer.limit(limit);
        }
        // mark 删除 app read buffer已读部分
        appReadBuffer.compact();
        // mark 返回读取的实际长度
        return remaining;
    }

    protected int netReadBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    protected int netWriteBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    protected int applicationBufferSize() {
        return sslEngine.getSession().getApplicationBufferSize();
    }

    protected ByteBuffer netReadBuffer() {
        return netReadBuffer;
    }

    // Visibility for testing
    protected ByteBuffer appReadBuffer() {
        return appReadBuffer;
    }

    /**
     * SSL exceptions are propagated as authentication failures so that clients can avoid
     * retries and report the failure. If `flush` is true, exceptions are propagated after
     * any pending outgoing bytes are flushed to ensure that the peer is notified of the failure.
     */
    private void handshakeFailure(SSLException sslException, boolean flush) throws IOException {
        //Release all resources such as internal buffers that SSLEngine is managing
        log.debug("SSL Handshake failed", sslException);
        sslEngine.closeOutbound();
        try {
            sslEngine.closeInbound();
        } catch (SSLException e) {
            log.debug("SSLEngine.closeInBound() raised an exception.", e);
        }

        state = State.HANDSHAKE_FAILED;
        handshakeException = new SslAuthenticationException("SSL handshake failed", sslException);

        // Attempt to flush any outgoing bytes. If flush doesn't complete, delay exception handling until outgoing bytes
        // are flushed. If write fails because remote end has closed the channel, log the I/O exception and  continue to
        // handle the handshake failure as an authentication exception.
        if (!flush || handshakeWrapAfterFailure(flush))
            throw handshakeException;
        else
            log.debug("Delay propagation of handshake exception till {} bytes remaining are flushed", netWriteBuffer.remaining());
    }

    // SSL handshake failures are typically thrown as SSLHandshakeException, SSLProtocolException,
    // SSLPeerUnverifiedException or SSLKeyException if the cause is known. These exceptions indicate
    // authentication failures (e.g. configuration errors) which should not be retried. But the SSL engine
    // may also throw exceptions using the base class SSLException in a few cases:
    //   a) If there are no matching ciphers or TLS version or the private key is invalid, client will be
    //      unable to process the server message and an SSLException is thrown:
    //      javax.net.ssl.SSLException: Unrecognized SSL message, plaintext connection?
    //   b) If server closes the connection gracefully during handshake, client may receive close_notify
    //      and and an SSLException is thrown:
    //      javax.net.ssl.SSLException: Received close_notify during handshake
    // We want to handle a) as a non-retriable SslAuthenticationException and b) as a retriable IOException.
    // To do this we need to rely on the exception string. Since it is safer to throw a retriable exception
    // when we are not sure, we will treat only the first exception string as a handshake exception.
    private void maybeProcessHandshakeFailure(SSLException sslException, boolean flush, IOException ioException) throws IOException {
        if (sslException instanceof SSLHandshakeException || sslException instanceof SSLProtocolException ||
                sslException instanceof SSLPeerUnverifiedException || sslException instanceof SSLKeyException ||
                sslException.getMessage().contains("Unrecognized SSL message") ||
                sslException.getMessage().contains("Received fatal alert: "))
            handshakeFailure(sslException, flush);
        else if (ioException == null)
            throw sslException;
        else {
            log.debug("SSLException while unwrapping data after IOException, original IOException will be propagated", sslException);
            throw ioException;
        }
    }

    // If handshake has already failed, throw the authentication exception.
    private void maybeThrowSslAuthenticationException() {
        if (handshakeException != null)
            throw handshakeException;
    }

    /**
     * Perform handshake wrap after an SSLException or any IOException.
     *
     * If `doWrite=false`, we are processing IOException after peer has disconnected, so we
     * cannot send any more data. We perform any pending wraps so that we can unwrap any
     * peer data that is already available.
     *
     * If `doWrite=true`, we are processing SSLException, we perform wrap and flush
     * any data to notify the peer of the handshake failure.
     *
     * Returns true if no more wrap is required and any data is flushed or discarded.
     */
    private boolean handshakeWrapAfterFailure(boolean doWrite) {
        try {
            log.trace("handshakeWrapAfterFailure status {} doWrite {}", handshakeStatus, doWrite);
            while (handshakeStatus == HandshakeStatus.NEED_WRAP && (!doWrite || flush(netWriteBuffer))) {
                if (!doWrite)
                    clearWriteBuffer();
                handshakeWrap(doWrite);
            }
        } catch (Exception e) {
            log.debug("Failed to wrap and flush all bytes before closing channel", e);
            clearWriteBuffer();
        }
        if (!doWrite)
            clearWriteBuffer();
        return !netWriteBuffer.hasRemaining();
    }

    private void clearWriteBuffer() {
        if (netWriteBuffer.hasRemaining())
            log.debug("Discarding write buffer {} since peer has disconnected", netWriteBuffer);
        netWriteBuffer.position(0);
        netWriteBuffer.limit(0);
    }

    @Override
    public boolean isMute() {
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    @Override
    public boolean hasBytesBuffered() {
        return hasBytesBuffered;
    }

    /**
     * 更新是否传输层是buffer中是否还有剩余数据未读取标志
     * 此方法用于判断在网络读取缓冲区或应用读取缓冲区中是否仍有可读的字节。
     * 目的是决定是否应进行更多的读取尝试，或者系统是否应等待更多数据可用。
     *
     * @param madeProgress 表示在前一次读取尝试中是否取得进展。如果取得了进展，意味着至少读取了一个字节。
     *                     此参数有助于确定缓冲区中是否仍包含未读数据。
     */
    // 更新 `hasBytesBuffered` 状态。如果从网络读取了任何字节，
    // 或者从读取操作返回了数据，如果缓冲区中仍有剩余的数据，则将 `hasBytesBuffered` 设置为 true。
    // 如果没有，由于没有更多数据可读，`hasBytesBuffered` 将被设置为 false，因此无法取得进一步的进展。
    private void updateBytesBuffered(boolean madeProgress) {
        // mark 如果本次有读取进展 但是net read buffer 或者 app read buffer中还有数据可读 则标记为 hasBytesBuffered
        if (madeProgress)
            hasBytesBuffered = netReadBuffer.position() != 0 || appReadBuffer.position() != 0;
        else
            // mark 如果没有取得进展，这表明当前缓冲区为空，在更多数据到达之前无法进行进一步的读取。
            hasBytesBuffered = false;
    }


    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        if (state == State.CLOSING)
            throw closingException();
        if (state != State.READY)
            return 0;

        if (!flush(netWriteBuffer))
            return 0;

        long channelSize = fileChannel.size();
        if (position > channelSize)
            return 0;
        int totalBytesToWrite = (int) Math.min(Math.min(count, channelSize - position), Integer.MAX_VALUE);

        if (fileChannelBuffer == null) {
            // Pick a size that allows for reasonably efficient disk reads, keeps the memory overhead per connection
            // manageable and can typically be drained in a single `write` call. The `netWriteBuffer` is typically 16k
            // and the socket send buffer is 100k by default, so 32k is a good number given the mentioned trade-offs.
            int transferSize = 32768;
            // Allocate a direct buffer to avoid one heap to heap buffer copy. SSLEngine copies the source
            // buffer (fileChannelBuffer) to the destination buffer (netWriteBuffer) and then encrypts in-place.
            // FileChannel.read() to a heap buffer requires a copy from a direct buffer to a heap buffer, which is not
            // useful here.
            fileChannelBuffer = ByteBuffer.allocateDirect(transferSize);
            // The loop below drains any remaining bytes from the buffer before reading from disk, so we ensure there
            // are no remaining bytes in the empty buffer
            fileChannelBuffer.position(fileChannelBuffer.limit());
        }

        int totalBytesWritten = 0;
        long pos = position;
        try {
            while (totalBytesWritten < totalBytesToWrite) {
                if (!fileChannelBuffer.hasRemaining()) {
                    fileChannelBuffer.clear();
                    int bytesRemaining = totalBytesToWrite - totalBytesWritten;
                    if (bytesRemaining < fileChannelBuffer.limit())
                        fileChannelBuffer.limit(bytesRemaining);
                    int bytesRead = fileChannel.read(fileChannelBuffer, pos);
                    if (bytesRead <= 0)
                        break;
                    fileChannelBuffer.flip();
                }
                int networkBytesWritten = write(fileChannelBuffer);
                totalBytesWritten += networkBytesWritten;
                // In the case of a partial write we only return the written bytes to the caller. As a result, the
                // `position` passed in the next `transferFrom` call won't include the bytes remaining in
                // `fileChannelBuffer`. By draining `fileChannelBuffer` first, we ensure we update `pos` before
                // we invoke `fileChannel.read`.
                if (fileChannelBuffer.hasRemaining())
                    break;
                pos += networkBytesWritten;
            }
            return totalBytesWritten;
        } catch (IOException e) {
            if (totalBytesWritten > 0)
                return totalBytesWritten;
            throw e;
        }
    }
}
