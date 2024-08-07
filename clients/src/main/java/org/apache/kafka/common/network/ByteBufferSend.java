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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 由字节缓冲区数组支持的发送
 */
public class ByteBufferSend implements Send {

    private final long size;
    protected final ByteBuffer[] buffers;
    private long remaining;
    private boolean pending = false;

    public ByteBufferSend(ByteBuffer... buffers) {
        this.buffers = buffers;
        // mark 计算整体的数据容量
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        // mark 初始化是size等于remaining
        this.size = remaining;
    }

    public ByteBufferSend(ByteBuffer[] buffers, long size) {
        this.buffers = buffers;
        this.size = size;
        this.remaining = size;
    }

    /**
     * 判断任务是否已完成
     * 通过检查剩余任务数量和是否有未处理的任务来确定任务是否已完成
     * 如果剩余任务数量为0且没有未处理的任务，则认为任务已完成
     *
     * @return true，如果任务已完成；否则返回false
     */
    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        /**
         * 将buffer数组使用对应的传输层写入到socket buffer中
         * 具体参见
         * SSL传输层 {@link SslTransportLayer#write(ByteBuffer[])}
         * 明文传输层 {@link PlaintextTransportLayer#write(ByteBuffer[])}
         */
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        // mark 重新计算整体剩余容量
        remaining -= written;
        // mark 检查传输层是否还有数据未写入到 socket buffer
        // mark 明文传输层是直接写入socket buffer 不存在pending情况
        // mark SSL传输层是将原buffer通过SSLEngin加密到net write buffer 再写入socket buffer 所以如果net write buffer有数据没有写入则pending
        // mark 这里说明在写入的时候发生了中断 很有可能是达到最大接收缓冲区大小限制
        pending = channel.hasPendingWrites();
        return written;
    }

    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return "ByteBufferSend(" +
            ", size=" + size +
            ", remaining=" + remaining +
            ", pending=" + pending +
            ')';
    }

    public static ByteBufferSend sizePrefixed(ByteBuffer buffer) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(0, buffer.remaining());
        return new ByteBufferSend(sizeBuffer, buffer);
    }
}
