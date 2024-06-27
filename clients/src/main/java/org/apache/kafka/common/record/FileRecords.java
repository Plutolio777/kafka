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
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Records} implementation backed by a file. An optional start and end position can be applied to this
 * instance to enable slicing a range of the log records.
 */
public class FileRecords extends AbstractRecords implements Closeable {
    private final boolean isSlice;
    private final int start;
    private final int end;

    private final Iterable<FileLogInputStream.FileChannelRecordBatch> batches;

    // mutable state
    private final AtomicInteger size;
    private final FileChannel channel;
    private volatile File file;

    /**
     * The {@code FileRecords.open} methods should be used instead of this constructor whenever possible.
     * The constructor is visible for tests.
     */
    FileRecords(File file,
                FileChannel channel,
                int start,
                int end,
                boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice; // 是否为视图切片
        this.size = new AtomicInteger();
        // mark 判断是否为视图切片
        if (isSlice) {
            // don't check the file size if this is just a slice view
            // mark 如果是视图切片 不设置position直接设置size
            size.set(end - start);
        } else {
            // mark 检查文件大小是否超过阈值
            if (channel.size() > Integer.MAX_VALUE)
                throw new KafkaException("The size of segment " + file + " (" + channel.size() +
                        ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);
            // mark 设置size
            int limit = Math.min((int) channel.size(), end);
            size.set(limit - start);

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            // mark 将文件指针设置到文件末尾
            channel.position(limit);
        }
        // mark 用于生成处理日志段的迭代器（返回一个迭代器工厂）
        batches = batchesFrom(start);
    }

    @Override
    public int sizeInBytes() {
        return size.get();
    }

    /**
     * Get the underlying file.
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * Read log batches into the given buffer until there are no bytes remaining in the buffer or the end of the file
     * is reached.
     *
     * @param buffer The buffer to write the batches to
     * @param position Position in the buffer to read from
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     * possible exceptions
     */
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        Utils.readFully(channel, buffer, position + this.start);
        buffer.flip();
    }

    /**
     * Return a slice of records from this instance, which is a view into this set starting from the given position
     * and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileRecords slice(int position, int size) throws IOException {
        int availableBytes = availableBytes(position, size);
        int startPosition = this.start + position;
        return new FileRecords(file, channel, startPosition, startPosition + availableBytes, true);
    }

    /**
     * Return a slice of records from this instance, the difference with {@link FileRecords#slice(int, int)} is
     * that the position is not necessarily on an offset boundary.
     *
     * This method is reserved for cases where offset alignment is not necessary, such as in the replication of raft
     * snapshots.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A unaligned slice of records on this message set limited based on the given position and size
     */
    public UnalignedFileRecords sliceUnaligned(int position, int size) {
        int availableBytes = availableBytes(position, size);
        return new UnalignedFileRecords(channel, this.start + position, availableBytes);
    }

    private int availableBytes(int position, int size) {
        // Cache current size in case concurrent write changes it
        int currentSizeInBytes = sizeInBytes();

        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position + " in read from " + this);
        if (position > currentSizeInBytes - start)
            throw new IllegalArgumentException("Slice from position " + position + " exceeds end position of " + this);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);

        int end = this.start + position + size;
        // Handle integer overflow or if end is beyond the end of the file
        if (end < 0 || end > start + currentSizeInBytes)
            end = this.start + currentSizeInBytes;
        return end - (this.start + position);
    }

    /**
     * Append a set of records to the file. This method is not thread-safe and must be
     * protected with a lock.
     *
     * @param records The records to append
     * @return the number of bytes written to the underlying file
     */
    public int append(MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());

        int written = records.writeFullyTo(channel);
        size.getAndAdd(written);
        return written;
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * Close this record set
     */
    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }

    /**
     * Close file handlers used by the FileChannel but don't write to disk. This is used when the disk may have failed
     */
    public void closeHandlers() throws IOException {
        channel.close();
    }

    /**
     * Delete this message set from the filesystem
     * @throws IOException if deletion fails due to an I/O error
     * @return  {@code true} if the file was deleted by this method; {@code false} if the file could not be deleted
     *          because it did not exist
     */
    public boolean deleteIfExists() throws IOException {
        Utils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    /**
     * Trim file when close or roll to next file
     */
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * Update the parent directory (to be used with caution since this does not reopen the file channel)
     * @param parentDir The new parent directory
     */
    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    /**
     * Rename the file that backs this message set
     * @throws IOException if rename fails.
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    /**
     * mark 将log截断 只保留到targetSize
     * 将此文件消息集截断到给定的字节大小。请注意，此 API 不会检查给定的大小是否落在有效的消息边界上。
     * 在某些版本的 JDK 中，截断到与文件消息集相同的大小会导致文件的修改时间（mtime）更新，
     * 因此仅在目标大小小于底层 FileChannel 大小时执行截断操作。
     * 预期在调用此函数时不会有其他线程对日志进行写操作。
     *
     * @param targetSize 要截断到的大小。必须在 0 和 sizeInBytes 之间。
     * @return 截断掉的字节数
     * @throws IOException 如果发生 I/O 错误
     * @throws KafkaException 如果目标大小无效或超出范围
     */
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        // mark 如果大于原始大小则截断失败
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("尝试将日志段 " + file + " 截断到 " + targetSize + " 字节失败，" +
                    "此日志段的大小为 " + originalSize + " 字节。");
        // mark 调用FileChannel的truncate方法进行截断
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }
    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        ConvertedRecords<MemoryRecords> convertedRecords = RecordsUtil.downConvert(batches, toMagic, firstOffset, time);
        if (convertedRecords.recordConversionStats().numRecordsConverted() == 0) {
            // This indicates that the message is too large, which means that the buffer is not large
            // enough to hold a full record batch. We just return all the bytes in this instance.
            // Even though the record batch does not have the right format version, we expect old clients
            // to raise an error to the user after reading the record batch size and seeing that there
            // are not enough available bytes in the response to read it fully. Note that this is
            // only possible prior to KIP-74, after which the broker was changed to always return at least
            // one full record batch, even if it requires exceeding the max fetch size requested by the client.
            return new ConvertedRecords<>(this, RecordConversionStats.EMPTY);
        } else {
            return convertedRecords;
        }
    }

    @Override
    public long writeTo(TransferableChannel destChannel, long offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end) - start;
        int oldSize = sizeInBytes();
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));

        long position = start + offset;
        long count = Math.min(length, oldSize - offset);
        return destChannel.transferFrom(channel, position, count);
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position and the size of the message (including log overhead) at the returned offset. If
     * no such offsets are found, return null.
     *
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        for (FileChannelRecordBatch batch : batchesFrom(startingPosition)) {
            long offset = batch.lastOffset();
            if (offset >= targetOffset)
                return new LogOffsetPosition(offset, batch.position(), batch.sizeInBytes());
        }
        return null;
    }

    /**
     * Search forward for the first message that meets the following requirements:
     * - Message's timestamp is greater than or equals to the targetTimestamp.
     * - Message's position in the log file is greater than or equals to the startingPosition.
     * - Message's offset is greater than or equals to the startingOffset.
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @param startingOffset The starting offset to search.
     * @return The timestamp and offset of the message found. Null if no message is found.
     */
    public TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset) {
        for (RecordBatch batch : batchesFrom(startingPosition)) {
            if (batch.maxTimestamp() >= targetTimestamp) {
                // We found a message
                for (Record record : batch) {
                    long timestamp = record.timestamp();
                    if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                        return new TimestampAndOffset(timestamp, record.offset(),
                                maybeLeaderEpoch(batch.partitionLeaderEpoch()));
                }
            }
        }
        return null;
    }

    /**
     * Return the largest timestamp of the messages after a given position in this file message set.
     * @param startingPosition The starting position.
     * @return The largest timestamp of the messages after the given position.
     */
    public TimestampAndOffset largestTimestampAfter(int startingPosition) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        int leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH;

        for (RecordBatch batch : batchesFrom(startingPosition)) {
            long timestamp = batch.maxTimestamp();
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                offsetOfMaxTimestamp = batch.lastOffset();
                leaderEpochOfMaxTimestamp = batch.partitionLeaderEpoch();
            }
        }
        return new TimestampAndOffset(maxTimestamp, offsetOfMaxTimestamp,
                maybeLeaderEpoch(leaderEpochOfMaxTimestamp));
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * Get an iterator over the record batches in the file. Note that the batches are
     * backed by the open file channel. When the channel is closed (i.e. when this instance
     * is closed), the batches will generally no longer be readable.
     * @return An iterator over the batches
     */
    @Override
    public Iterable<FileChannelRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "FileRecords(size=" + sizeInBytes() +
                ", file=" + file +
                ", start=" + start +
                ", end=" + end +
                ")";
    }

    /**
     * Get an iterator over the record batches in the file, starting at a specific position. This is similar to
     * {@link #batches()} except that callers specify a particular position to start reading the batches from. This
     * method must be used with caution: the start position passed in must be a known start of a batch.
     * @param start The position to start record iteration from; must be a known position for start of a batch
     * @return An iterator over batches starting from {@code start}
     */
    public Iterable<FileChannelRecordBatch> batchesFrom(final int start) {
        return () -> batchIterator(start);
    }

    @Override
    public AbstractIterator<FileChannelRecordBatch> batchIterator() {
        return batchIterator(start);
    }

    private AbstractIterator<FileChannelRecordBatch> batchIterator(int start) {
        final int end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        FileLogInputStream inputStream = new FileLogInputStream(this, start, end);
        return new RecordBatchIterator<>(inputStream);
    }

    /**
     * mark 打开一个 FileRecords 对象。
     * <p>
     * 该方法根据指定的参数打开一个文件，并返回一个 FileRecords 对象。
     * 它首先调用 openChannel 方法打开文件通道，然后根据文件是否已经存在以及是否预分配空间，
     * 决定 FileRecords 对象的结束位置。
     *
     * @param file              要打开的文件。
     * @param mutable           是否允许文件被修改。
     * @param fileAlreadyExists 文件是否已经存在。
     * @param initFileSize      初始化文件大小（如果需要预分配空间）。
     * @param preallocate       是否预分配文件空间。
     * @return 返回一个表示文件记录的 FileRecords 对象。
     * @throws IOException 如果在打开文件或通道时发生 I/O 错误。
     */
    public static FileRecords open(File file,
                                   boolean mutable,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        return new FileRecords(file, channel, 0, end, false);
    }

    public static FileRecords open(File file,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileRecords open(File file) throws IOException {
        return open(file, true);
    }

    /**
     * 打开给定文件的通道。
     * 对于 Windows NTFS 和一些旧的 LINUX 文件系统，设置 preallocate 为 true 并将 initFileSize 设为一个值
     * （例如 512 * 1025 * 1024）可以提高 Kafka 的生产性能。
     *
     * @param file 文件路径
     * @param mutable 是否可变
     * @param fileAlreadyExists 文件是否已经存在
     * @param initFileSize 用于预分配文件的大小，例如 512 * 1025 * 1024
     * @param preallocate 是否预分配文件，从配置中获取
     * @return 打开的文件通道
     * @throws IOException 如果发生 I/O 错误
     */
    private static FileChannel openChannel(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            // mark 使用NIO的内存映射文件IO通道 性能最佳
            if (fileAlreadyExists || !preallocate) {
                return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                // mark 传统随机读写IO通道
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(initFileSize);
                return randomAccessFile.getChannel();
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }

    public static class LogOffsetPosition {
        public final long offset;
        public final int position;
        public final int size;

        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset &&
                    position == that.position &&
                    size == that.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(offset);
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition(" +
                    "offset=" + offset +
                    ", position=" + position +
                    ", size=" + size +
                    ')';
        }
    }

    public static class TimestampAndOffset {
        public final long timestamp;
        public final long offset;
        public final Optional<Integer> leaderEpoch;

        public TimestampAndOffset(long timestamp, long offset, Optional<Integer> leaderEpoch) {
            this.timestamp = timestamp;
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampAndOffset that = (TimestampAndOffset) o;
            return timestamp == that.timestamp &&
                    offset == that.offset &&
                    Objects.equals(leaderEpoch, that.leaderEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, offset, leaderEpoch);
        }

        @Override
        public String toString() {
            return "TimestampAndOffset(" +
                    "timestamp=" + timestamp +
                    ", offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ')';
        }
    }
}
