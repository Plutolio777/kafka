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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;

public interface Writable {
    void writeByte(byte val);
    void writeShort(short val);
    void writeInt(int val);
    void writeLong(long val);
    void writeDouble(double val);
    void writeByteArray(byte[] arr);
    void writeUnsignedVarint(int i);
    void writeByteBuffer(ByteBuffer buf);
    void writeVarint(int i);
    void writeVarlong(long i);

    /**
     * 将给定的记录写入输出。
     * 此方法支持直接将MemoryRecords实例写入输出，
     * 通过提取底层的ByteBuffer并调用writeByteBuffer方法来实现。
     *
     * @param records 要写入的BaseRecords实例
     */
    default void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            MemoryRecords memRecords = (MemoryRecords) records;
            writeByteBuffer(memRecords.buffer());
        } else {
            throw new UnsupportedOperationException("不支持的记录类型 " + records.getClass());
        }
    }

    /**
     * 将UUID对象写入输出。
     * 先写入UUID的高位部分，再写入低位部分。
     *
     * @param uuid 要写入的UUID对象
     */
    default void writeUuid(Uuid uuid) {
        // mark 先写入uuid的高位部分
        writeLong(uuid.getMostSignificantBits());
        // mark 再写入uuid的低位
        writeLong(uuid.getLeastSignificantBits());
    }

    /**
     * 写入一个无符号短整型数。
     *
     * @param i 要写入的无符号短整型数值
     */
    default void writeUnsignedShort(int i) {
        writeShort((short) i);
    }

    /**
     * 写入一个无符号整型数。
     *
     * @param i 要写入的无符号整型数值
     */
    default void writeUnsignedInt(long i) {
        writeInt((int) i);
    }

}
