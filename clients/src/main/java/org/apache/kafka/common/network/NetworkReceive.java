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

import org.apache.kafka.common.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * 大小分隔的接收，由 4 字节网络排序大小 N 后跟 N 字节内容组成
 */
public class NetworkReceive implements Receive {

    public static final String UNKNOWN_SOURCE = "";
    public static final int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // mark 挂载的通道ID
    private final String source;
    // mark 用于接收消息的头四个字节 表示消息大小
    private final ByteBuffer size;
    // mark 最大接收缓冲区大小
    private final int maxSize;
    // mark 用于生成buffer的内存池
    private final MemoryPool memoryPool;
    private int requestedBufferSize = -1;
    // mark 用于接收消息的payload 由内存池生成
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this(UNLIMITED, source);
        this.buffer = buffer;
    }

    public NetworkReceive(String source) {
        this(UNLIMITED, source);
    }

    public NetworkReceive(int maxSize, String source) {
        this(maxSize, source, MemoryPool.NONE);
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    /**
     * 检查是否完成数据传输。
     * <p>
     * 此方法用于判断数据传输是否已经完全结束。它通过检查两个条件来确定：
     * 1. 外部标尺缓冲区（size）是否已经没有剩余字节需要传输；
     * 2. 内部缓冲区（buffer）是否已经没有剩余字节需要传输。
     * 如果两个条件都满足，则认为数据传输已经完全结束。
     *
     * @return 如果数据传输已完成，则返回true；否则返回false。
     */
    @Override
    public boolean complete() {
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    /**
     * 从给定的传输层读取数据。
     * 此方法首先尝试读取一个整数，该整数表示接下来要读取的数据块的大小。
     * 如果数据块大小有效，则根据需要分配缓冲区，并继续读取数据到缓冲区中。
     *
     * @param channel 传输数据的传输层实例 {@link PlaintextTransportLayer}, {@link SslTransportLayer}
     * @return 本次读取操作总共读取的字节数。
     * @throws IOException             如果读取操作发生错误。
     * @throws EOFException            如果通道已经到达末尾，无法读取更多数据。
     * @throws InvalidReceiveException 如果读取到的数据大小不合法或超过最大允许大小。
     */
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        // mark 1.从消息中读取前4个字节 存入size中 这四个字节本次接收的数据大小
        if (size.hasRemaining()) {
            // mark 调用传输层的read方法读取数据
            // mark 1.PlaintextTransportLayer直接从socket buffer中读取
            // mark 2.SSLTransportLayer需要经过SSLEngine数据解密
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;

            // mark 如果size没有剩余空间了
            if (!size.hasRemaining()) {

                size.rewind();
                // mark 将size转换成int 表示接收数据的大小
                int receiveSize = size.getInt();
                // mark size解析不合法 抛出异常
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                // mark 超过最大接收大小 抛出异常
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                // mark 将size转换成int 表示接收数据的大小
                requestedBufferSize = receiveSize; //may be 0 for some payloads (SASL)
                // mark 如果接收数据大小为0 则将buffer指向空buffer 避免重新分配内存
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER;
                }
            }
        }

        // mark 如果当前Receive对象还没有生成buffer 并且 requestedBufferSize不为-1
        // mark 这里判断-1可能是第一次 读取size的时候 抛出异常 后续继续读取的时候还是会为-1
        if (buffer == null && requestedBufferSize != -1) { //we know the size we want but havent been able to allocate it yet
            // mark 从内存池中获取缓冲区
            buffer = memoryPool.tryAllocate(requestedBufferSize);
            // mark 如果获取失败 只记录异常
            if (buffer == null)
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
        }

        // mark 如果buffer不为空 则继续冲传输层读取payload内容
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }


    @Override
    public void close() throws IOException {
        if (buffer != null && buffer != EMPTY_BUFFER) {
            memoryPool.release(buffer);
            buffer = null;
        }
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

    public int bytesRead() {
        if (buffer == null)
            return size.position();
        return buffer.position() + size.position();
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with {@link NetworkSend#size()}
     */
    public int size() {
        return payload().limit() + size.limit();
    }

}
