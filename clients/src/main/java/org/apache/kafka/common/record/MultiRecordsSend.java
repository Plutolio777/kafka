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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransferableChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * A set of composite sends with nested {@link RecordsSend}, sent one after another
 */
public class MultiRecordsSend implements Send {
    private static final Logger log = LoggerFactory.getLogger(MultiRecordsSend.class);

    private final Queue<Send> sendQueue;
    private final long size;
    private Map<TopicPartition, RecordConversionStats> recordConversionStats;

    private long totalWritten = 0;
    private Send current;

    /**
     * Construct a MultiRecordsSend from a queue of Send objects. The queue will be consumed as the MultiRecordsSend
     * progresses (on completion, it will be empty).
     */
    public MultiRecordsSend(Queue<Send> sends) {
        this.sendQueue = sends;

        long size = 0;
        for (Send send : sends)
            size += send.size();
        this.size = size;

        this.current = sendQueue.poll();
    }

    public MultiRecordsSend(Queue<Send> sends, long size) {
        this.sendQueue = sends;
        this.size = size;
        this.current = sendQueue.poll();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean completed() {
        return current == null;
    }

    // Visible for testing
    int numResidentSends() {
        int count = 0;
        if (current != null)
            count += 1;
        count += sendQueue.size();
        return count;
    }

    /**
     * 将当前请求数据写入给定的传输通道。
     * 如果请求已经完成，将抛出异常。
     *
     * @param channel 要写入数据的通道。
     * @return 此次调用中写入的总字节数。
     * @throws IOException    在写入操作期间发生I/O错误时抛出。
     * @throws KafkaException 当在已完成的请求上执行此操作时抛出。
     */
    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        // 确保请求未完成，否则抛出异常。
        if (completed())
            throw new KafkaException("无法在已完成的请求上执行此操作。");

        // 此次调用中写入的总字节数。
        int totalWrittenPerCall = 0;
        // 标记指示当前数据块是否已完全写入。
        boolean sendComplete;
        do {
            // 将当前数据块写入通道，并更新此次调用中写入的总字节数。
            long written = current.writeTo(channel);
            totalWrittenPerCall += written;
            // 检查当前数据块是否已完全写入。
            sendComplete = current.completed();
            // 如果当前数据块已完全写入，更新统计信息并准备写入下一个数据块。
            if (sendComplete) {
                updateRecordConversionStats(current);
                current = sendQueue.poll();
            }
        } while (!completed() && sendComplete); // 只要请求未完成且有数据可写，就继续写入。

        // 更新自请求开始以来写入的总字节数。
        totalWritten += totalWrittenPerCall;

        // 请求完成后，记录并检查预期总字节数与实际总字节数之间的差异。
        if (completed() && totalWritten != size)
            log.error("通过套接字发送的字节不匹配；预期：{} 实际：{}", size, totalWritten);

        // 追踪日志记录此次调用中写入的字节数、到目前为止写入的总字节数以及预期的总字节数。
        log.trace("作为多发送调用的一部分写入的字节数：{}，到目前为止写入的总字节数：{}，预期写入的字节数：{}",
                totalWrittenPerCall, totalWritten, size);

        return totalWrittenPerCall;
    }


    /**
     * Get any statistics that were recorded as part of executing this {@link MultiRecordsSend}.
     * @return Records processing statistics (could be null if no statistics were collected)
     */
    public Map<TopicPartition, RecordConversionStats> recordConversionStats() {
        return recordConversionStats;
    }

    @Override
    public String toString() {
        return "MultiRecordsSend(" +
            "size=" + size +
            ", totalWritten=" + totalWritten +
            ')';
    }

    private void updateRecordConversionStats(Send completedSend) {
        // The underlying send might have accumulated statistics that need to be recorded. For example,
        // LazyDownConversionRecordsSend accumulates statistics related to the number of bytes down-converted, the amount
        // of temporary memory used for down-conversion, etc. Pull out any such statistics from the underlying send
        // and fold it up appropriately.
        if (completedSend instanceof LazyDownConversionRecordsSend) {
            if (recordConversionStats == null)
                recordConversionStats = new HashMap<>();
            LazyDownConversionRecordsSend lazyRecordsSend = (LazyDownConversionRecordsSend) completedSend;
            recordConversionStats.put(lazyRecordsSend.topicPartition(), lazyRecordsSend.recordConversionStats());
        }
    }
}
