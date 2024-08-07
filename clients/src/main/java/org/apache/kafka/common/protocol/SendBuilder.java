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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MultiRecordsSend;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * 该类提供了一种从生成的 {@link org.apache.kafka.common.protocol.ApiMessage} 类型
 * 构建 {@link Send} 对象以用于网络传输的方法，而无需为“零拷贝”字段分配新的空间
 * （参见 {@link #writeByteBuffer(ByteBuffer)} 和 {@link #writeRecords(BaseRecords)}）。
 * 参见 {@link org.apache.kafka.common.requests.EnvelopeRequest#toSend(RequestHeader)}
 * 以获取示例用法。
 */
public class SendBuilder implements Writable {
    // mark buffer
    private final ByteBuffer buffer;

    private final Queue<Send> sends = new ArrayDeque<>(1);
    private long sizeOfSends = 0;
    // mark 用于保存buffer切片
    private final List<ByteBuffer> buffers = new ArrayList<>();
    private long sizeOfBuffers = 0;

    SendBuilder(int size) {
        // mark 分配buffer内存
        this.buffer = ByteBuffer.allocate(size);
        // mark 标记起始位置 后续使用reset进行回滚
        this.buffer.mark();
    }

    @Override
    public void writeByte(byte val) {
        buffer.put(val);
    }

    @Override
    public void writeShort(short val) {
        buffer.putShort(val);
    }

    @Override
    public void writeInt(int val) {
        buffer.putInt(val);
    }

    @Override
    public void writeLong(long val) {
        buffer.putLong(val);
    }

    @Override
    public void writeDouble(double val) {
        buffer.putDouble(val);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        buffer.put(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        // mark 整数都是使用proto buffer中的可变长算法
        ByteUtils.writeUnsignedVarint(i, buffer);
    }

    /**
     * 写入一个字节缓冲区。对底层缓冲区的引用将
     * 保留在{@link #build()}的结果中。
     *
     * @param buf 要写入的缓冲区
     */
    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        flushPendingBuffer();
        addBuffer(buf.duplicate());
    }

    @Override
    public void writeVarint(int i) {
        ByteUtils.writeVarint(i, buffer);
    }

    @Override
    public void writeVarlong(long i) {
        ByteUtils.writeVarlong(i, buffer);
    }

    /**
     * 将ByteBuffer对象添加到缓冲区列表中，并更新缓冲区总大小。
     *
     * 此方法用于管理一个ByteBuffer对象的集合，同时跟踪这些缓冲区的总大小。
     * 它是私有的，说明它只应该在类的内部使用，这符合封装的原则。
     *
     * @param buffer 要添加到缓冲区列表的ByteBuffer对象。
     *               该参数不能为null，否则会抛出NullPointerException。
     *               假设buffer是经过适当初始化和准备的，因此不需要在此处进行检查或处理。
     *
     *               添加buffer到buffers列表中，以便以后可以作为一个连续的缓冲区链使用。
     *               这对于处理大型数据集，尤其是通过网络传输的数据非常有用。
     *
     *               通过调用buffer.remaining()方法来获取buffer尚未被读取或写入的字节数量，
     *               并将这个值添加到sizeOfBuffers中，以保持对所有缓冲区总大小的跟踪。
     *               这对于管理缓冲区的内存使用和优化数据处理性能非常重要。
     */
    private void addBuffer(ByteBuffer buffer) {
        buffers.add(buffer);
        sizeOfBuffers += buffer.remaining();
    }


    private void addSend(Send send) {
        sends.add(send);
        sizeOfSends += send.size();
    }

    private void clearBuffers() {
        buffers.clear();
        sizeOfBuffers = 0;
    }

    /**
     * 写入记录集。基础记录数据将在 {@link #build()} 的结果中保留。
     * 参见 {@link BaseRecords#toSend()}。
     *
     * @param records 要写入的记录
     */
    @Override
    public void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            flushPendingBuffer();
            addBuffer(((MemoryRecords) records).buffer());
        } else if (records instanceof UnalignedMemoryRecords) {
            flushPendingBuffer();
            addBuffer(((UnalignedMemoryRecords) records).buffer());
        } else {
            flushPendingSend();
            addSend(records.toSend());
        }
    }

    private void flushPendingSend() {
        flushPendingBuffer();
        if (!buffers.isEmpty()) {
            ByteBuffer[] byteBufferArray = buffers.toArray(new ByteBuffer[0]);
            addSend(new ByteBufferSend(byteBufferArray, sizeOfBuffers));
            clearBuffers();
        }
    }

    /**
     * 将当前缓冲区中的未处理数据刷新到目标位置。
     * 这个方法的目的是为了处理当前缓冲区中已经写入但尚未被处理的数据。它首先重置缓冲区的读指针，
     * 然后检查是否有新的数据需要被添加到处理队列中。如果存在未处理的数据，它将调整缓冲区的限制，
     * 并将这部分数据添加到处理队列中，最后重新配置缓冲区以准备接受新的数据。
     */
    private void flushPendingBuffer() {
        // mark 记录当前缓冲区的写指针位置
        int latestPosition = buffer.position();
        // mark 重置缓冲区的读指针
        buffer.reset();

        // mark 检查是否有新的数据需要处理(如果有新数据写入 reset之后指针在初始位置)
        if (latestPosition > buffer.position()) {
            // mark 设置缓冲区的限制为已写入的数据量，以便于切片操作
            buffer.limit(latestPosition);
            // mark 将当前缓冲区的切片添加到处理队列中
            addBuffer(buffer.slice());

            // 重新配置缓冲区，准备接受新的数据
            buffer.position(latestPosition);
            buffer.limit(buffer.capacity());
            // 标记当前位置，以便于后续的重置操作
            buffer.mark();
        }
    }


    public Send build() {
        flushPendingSend();

        if (sends.size() == 1) {
            return sends.poll();
        } else {
            return new MultiRecordsSend(sends, sizeOfSends);
        }
    }

    /**
     * 根据请求头和请求消息构建发送请求的对象。
     * 此方法封装了请求发送的构建过程，通过提取请求头中的相关信息和请求消息，
     * 调用另一个方法来构建最终的发送请求对象。
     *
     * @param header 请求头对象，包含请求的数据和版本信息。
     * @param apiRequest 请求消息对象，具体的消息内容。
     * @return Send对象，代表构建好的请求发送对象。
     */
    public static Send buildRequestSend(
        RequestHeader header,
        Message apiRequest
    ) {
        // mark 使用请求头和请求消息的信息构建发送请求对象
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiRequest,
            header.apiVersion()
        );
    }

    public static Send buildResponseSend(
        ResponseHeader header,
        Message apiResponse,
        short apiVersion
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiResponse,
            apiVersion
        );
    }

    /**
     * 构建一个Send对象，用于发送消息。
     * 此方法通过计算消息头和API消息的大小来构建一个Send对象，这包括序列化消息头和API消息，
     * 以及确定消息的总大小。这个过程是必要的，因为发送的消息需要知道其大小以便于网络传输。
     *
     * @param header 消息头，包含消息的基本信息。
     * @param headerVersion 消息头的版本号，用于兼容不同版本的系统。
     * @param apiMessage API消息，实际需要传输的数据。
     * @param apiVersion API消息的版本号，同样用于版本兼容性。
     * @return 返回一个构建好的Send对象，包含了消息的所有必要信息。
     */
    private static Send buildSend(
        Message header, // mark 请求头
        short headerVersion, // mark 请求头版本
        Message apiMessage, // mark 请求体
        short apiVersion // mark 请求体版本
    ) {
        // mark 创建一个对象序列化缓存，用于缓存序列化后的对象信息，以提高序列化效率。
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        // mark 创建一个消息大小累加器，用于计算消息的总大小。
        MessageSizeAccumulator messageSize = new MessageSizeAccumulator();

        // mark 计算消息头的大小，并将其累加到消息大小累加器中。
        header.addSize(messageSize, serializationCache, headerVersion);
        // mark 计算API消息的大小，并将其累加到消息大小累加器中。
        apiMessage.addSize(messageSize, serializationCache, apiVersion);

        // mark 根据消息总大小创建一个SendBuilder对象，用于构建发送消息的对象。(+4是存后面的消息总大小)
        SendBuilder builder = new SendBuilder(messageSize.sizeExcludingZeroCopy() + 4);
        /**
         * 这个size最终会被 {@link org.apache.kafka.common.network.NetworkReceive#size} 进行接收
         */
        builder.writeInt(messageSize.totalSize());
        // mark 序列化并写入消息头。
        header.write(builder, serializationCache, headerVersion);
        // mark 序列化并写入API消息。
        apiMessage.write(builder, serializationCache, apiVersion);

        // 构建并返回最终的Send对象。
        return builder.build();
    }


}
