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
package org.apache.kafka.common.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classes exposes low-level methods for reading/writing from byte streams or buffers.
 */
public final class ByteUtils {

    public static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(new byte[0]);

    private ByteUtils() {}

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return in.read()
                | (in.read() << 8)
                | (in.read() << 16)
                | (in.read() << 24);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value);
        out.write(value >>> 8);
        out.write(value >>> 16);
        out.write(value >>> 24);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readUnsignedVarint(DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return value;
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readVarint(DataInput in) throws IOException {
        int value = readUnsignedVarint(in);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readVarlong(DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readVarlong(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param in The input to read from
     * @return The double value read
     */
    public static double readDouble(DataInput in) throws IOException {
        return in.readDouble();
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     */
    public static double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /**
     * 将给定的整数按照 Google Protocol Buffers 的变长无符号编码规则写入缓冲区。
     * 此编码方式用于高效存储小到中等范围的整数，根据数值大小占用1到10个字节。
     *
     * @param value 要写入的值。这是一个有符号整数，但会被当作无符号值进行处理。
     * @param out 写入的目标，实现 DataOutput 接口。
     * @throws IOException 如果在写入过程中发生 I/O 错误。
     */
    public static void writeUnsignedVarint(int value, DataOutput out) throws IOException {
        // 当 value 的最高位为 1 时，继续循环，将其转换为字节并写入输出流
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80); // 将 value 的低 7 位与 0x80 相或，标志位为 1
            out.writeByte(b); // 写入字节
            value >>>= 7; // value 右移 7 位，准备处理下一个字节
        }
        // 当 value 的最高位为 0 时，表示变长编码完成，将剩余的 value 写入输出流
        out.writeByte((byte) value);
    }


    /**
     * 将给定的整数使用变长zig-zag编码写入输出中。这种编码方式来自
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>。
     *
     * @param value 需要写入的整数值。
     * @param out 写入的目标 DataOutput。
     * @throws IOException 如果在写入过程中发生IO异常。
     */
    public static void writeVarint(int value, DataOutput out) throws IOException {
        // lyj 对整数进行zig-zag编码并写入为无符号变长整数
        writeUnsignedVarint((value << 1) ^ (value >> 31), out);
    }


    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    }

    /**
     * 将给定的整数按照变量长度的zig-zag编码从 Google Protocol Buffers 中描述的方式写入输出。
     * 此编码方式适用于高效地序列化变长整数，特别是在数据中有许多小整数但偶尔出现大整数的情况下。
     *
     * @param value 要写入的值
     * @param out 要写入的数据输出
     * @throws IOException 如果在写入过程中发生IO异常
     */
    public static void writeVarlong(long value, DataOutput out) throws IOException {
        // 使用zig-zag编码转换值，以确保正数和负数可以使用相同的基本模式进行编码
        long v = (value << 1) ^ (value >> 63);

        // 循环将值按照变长编码格式写入输出，直到整个值都被编码完成
        while ((v & 0xffffffffffffff80L) != 0L) {
            // 将每7位作为一个字节进行编码，并设置最高位为1，表示还有更多的字节即将跟随
            out.writeByte(((int) v & 0x7f) | 0x80);
            v >>>= 7; // 无符号右移7位，准备处理下一个7位组
        }

        // 编码最后的字节，最高位为0，表示这是最后一个字节
        out.writeByte((byte) v);
    }


    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeDouble(double value, DataOutput out) throws IOException {
        out.writeDouble(value);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeDouble(double value, ByteBuffer buffer) {
        buffer.putDouble(value);
    }

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * @param value The signed value
     *
     * @see #writeUnsignedVarint(int, DataOutput)
     */
    public static int sizeOfUnsignedVarint(int value) {
        // Protocol buffers varint encoding is variable length, with a minimum of 1 byte
        // (for zero). The values themselves are not important. What's important here is
        // any leading zero bits are dropped from output. We can use this leading zero
        // count w/ fast intrinsic to calc the output length directly.

        // Test cases verify this matches the output for loop logic exactly.

        // return (38 - leadingZeros) / 7 + leadingZeros / 32;

        // The above formula provides the implementation, but the Java encoding is suboptimal
        // when we have a narrow range of integers, so we can do better manually

        int leadingZeros = Integer.numberOfLeadingZeros(value);
        int leadingZerosBelow38DividedBy7 = ((38 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 5);
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarint(int value) {
        return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     * @see #sizeOfUnsignedVarint(int)
     */
    public static int sizeOfVarlong(long value) {
        long v = (value << 1) ^ (value >> 63);

        // For implementation notes @see #sizeOfUnsignedVarint(int)
        // Similar logic is applied to allow for 64bit input -> 1-9byte output.
        // return (70 - leadingZeros) / 7 + leadingZeros / 64;

        int leadingZeros = Long.numberOfLeadingZeros(v);
        int leadingZerosBelow70DividedBy7 = ((70 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow70DividedBy7 + (leadingZeros >>> 6);
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
