/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
// Avoid shadowing mutable `file` in AbstractIndex
class OffsetIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex(_file, baseOffset, maxIndexSize, writable) {
  import OffsetIndex._

  // mark 偏移量索引 每一个索引条目为8个字节 前4个字节为相对偏移量 后四个字节为消息在segment文件中的物理地址
  override def entrySize = 8

  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, " +
    s"maxIndexSize = $maxIndexSize, entries = ${_entries}, lastOffset = ${_lastOffset}, file position = ${mmap.position()}")

  /**
   * Index Entry 是由OffsetPosition表示
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        // mark 返回初始entry
        case 0 => OffsetPosition(baseOffset, 0)

        case s => parseEntry(mmap, s - 1)
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
   * Find the largest offset less than or equal to the given targetOffset
   * and return a pair holding this offset and its corresponding physical file position.
   *
   * @param targetOffset The offset to look up.
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        parseEntry(idx, slot)
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
   */
  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(idx, slot))
    }
  }

  /**
   * 从 mmap 中获取相对偏移量。
   * index的entry为8字节 前4字节为相对偏移量所以直接getInt就行
   *
   * @param buffer ByteBuffer 对象，实际上是AbstractIndex中的mmap。
   * @param n      要解析的条目的索引位置。
   * @return 该条目的相对偏移量。
   */
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = {
    // 从指定索引位置 n 处读取一个整数作为相对偏移量
    buffer.getInt(n * entrySize)
  }

  /**
   * 从 mmap 中获取物理位置。
   * index的entry为8字节 后4字节为消息的物理地址
   *
   * @param buffer ByteBuffer 对象，实际上是AbstractIndex中的mmap。
   * @param n      要解析的条目的索引位置。
   * @return 该条目的物理位置。
   */
  private def physical(buffer: ByteBuffer, n: Int): Int = {
    // 从指定索引位置 n 处加 4 字节偏移读取一个整数作为物理位置
    buffer.getInt(n * entrySize + 4)
  }

  /**
   * 解析索引条目并返回相应的偏移位置。
   *
   * @param buffer ByteBuffer 对象，包含索引文件的数据。
   * @param n      要解析的条目的索引位置。
   * @return 解析后的 OffsetPosition 对象，包含条目的偏移量和物理位置。
   */
  override protected def parseEntry(buffer: ByteBuffer, n: Int): OffsetPosition = {
    // mark 通过相对偏移量和物理位置计算条目的实际偏移量和位置
    OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if (n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from index ${file.getAbsolutePath}, " +
          s"which has size ${_entries}.")
      parseEntry(mmap, n)
    }
  }

  /**
   * mark 添加偏移量索引
   *
   * @param offset   要追加的条目的偏移量
   * @param position 要追加的条目的位置
   * @throws kafka.common.IndexOffsetOverflowException 如果偏移量导致索引偏移量溢出
   * @throws InvalidOffsetException                    如果提供的偏移量不大于最后一个偏移量
   */
  def append(offset: Long, position: Int): Unit = {
    inLock(lock) {
      // mark 检查索引文件是否已满
      require(!isFull, "尝试向已满的索引追加条目 (大小 = " + _entries + ")。")
      if (_entries == 0 || offset > _lastOffset) {
        trace(s"将索引条目 $offset => $position 添加到 ${file.getAbsolutePath}")
        // mark 通过mmap向文件中写入offset
        mmap.putInt(relativeOffset(offset))
        // mark 通过mmap向文件中写入物理地址
        mmap.putInt(position)
        _entries += 1
        _lastOffset = offset
        require(_entries * entrySize == mmap.position(), s"$entries 条目，但索引中文件位置是 ${mmap.position()}。")
      } else {
        throw new InvalidOffsetException(s"尝试向位置 $entries 追加偏移量 ($offset) 不大于" +
          s" 最后追加的偏移量 (${_lastOffset}) 到 ${file.getAbsolutePath}。")
      }
    }
  }

  override def truncate(): Unit = truncateToEntries(0)

  override def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int): Unit = {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastOffset = lastEntry.offset
      debug(s"Truncated index ${file.getAbsolutePath} to $entries entries;" +
        s" position is now ${mmap.position()} and last offset is now ${_lastOffset}")
    }
  }

  override def sanityCheck(): Unit = {
    if (_entries != 0 && _lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size " +
        s"but the last offset is ${_lastOffset} which is less than the base offset $baseOffset.")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Index file ${file.getAbsolutePath} is corrupt, found $length bytes which is " +
        s"neither positive nor a multiple of $entrySize.")
  }

}

object OffsetIndex extends Logging {
  override val loggerName: String = classOf[OffsetIndex].getName
}
