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

import java.io.{Closeable, File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{ByteBufferUnmapper, OperatingSystem, Utils}

/**
 * The abstract index class which holds entry format agnostic methods.
 *
 * @param _file The index file
 * @param baseOffset the base offset of the segment that this index is corresponding to.
 * @param maxIndexSize The maximum index size in bytes.
 */
abstract class AbstractIndex(@volatile private var _file: File, val baseOffset: Long, val maxIndexSize: Int = -1,
                             val writable: Boolean) extends Closeable {
  import AbstractIndex._

  // Length of the index file
  @volatile
  private var _length: Long = _
  protected def entrySize: Int

  /*
   mark kafka关于冷热数据处理的底层原理（尽可能的让查找命中页缓存）
   Kafka mmaps index files into memory, and all the read / write operations of the index is through OS page cache. This
   avoids blocked disk I/O in most cases.

   To the extent of our knowledge, all the modern operating systems use LRU policy or its variants to manage page
   cache. Kafka always appends to the end of the index file, and almost all the index lookups (typically from in-sync
   followers or consumers) are very close to the end of the index. So, the LRU cache replacement policy should work very
   well with Kafka's index access pattern.

   However, when looking up index, the standard binary search algorithm is not cache friendly, and can cause unnecessary
   page faults (the thread is blocked to wait for reading some index entries from hard disk, as those entries are not
   cached in the page cache).

   For example, in an index with 13 pages, to lookup an entry in the last page (page #12), the standard binary search
   algorithm will read index entries in page #0, 6, 9, 11, and 12.
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12 |
   steps:       |1| | | | | |3| | |4|  |5 |2/6|
   In each page, there are hundreds log entries, corresponding to hundreds to thousands of kafka messages. When the
   index gradually growing from the 1st entry in page #12 to the last entry in page #12, all the write (append)
   operations are in page #12, and all the in-sync follower / consumer lookups read page #0,6,9,11,12. As these pages
   are always used in each in-sync lookup, we can assume these pages are fairly recently used, and are very likely to be
   in the page cache. When the index grows to page #13, the pages needed in a in-sync lookup change to #0, 7, 10, 12,
   and 13:
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13 |
   steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|
   Page #7 and page #10 have not been used for a very long time. They are much less likely to be in the page cache, than
   the other pages. The 1st lookup, after the 1st index entry in page #13 is appended, is likely to have to read page #7
   and page #10 from disk (page fault), which can take up to more than a second. In our test, this can cause the
   at-least-once produce latency to jump to about 1 second from a few ms.

   Here, we use a more cache-friendly lookup algorithm:
   if (target > indexEntry[end - N]) // if the target is in the last N entries of the index
      binarySearch(end - N, end)
   else
      binarySearch(begin, end - N)

   If possible, we only look up in the last N entries of the index. By choosing a proper constant N, all the in-sync
   lookups should go to the 1st branch. We call the last N entries the "warm" section. As we frequently look up in this
   relatively small section, the pages containing this section are more likely to be in the page cache.

   We set N (_warmEntries) to 8192, because
   1. This number is small enough to guarantee all the pages of the "warm" section is touched in every warm-section
      lookup. So that, the entire warm section is really "warm".
      When doing warm-section lookup, following 3 entries are always touched: indexEntry(end), indexEntry(end-N),
      and indexEntry((end*2 -N)/2). If page size >= 4096, all the warm-section pages (3 or fewer) are touched, when we
      touch those 3 entries. As of 2018, 4096 is the smallest page size for all the processors (x86-32, x86-64, MIPS,
      SPARC, Power, ARM etc.).
   2. This number is large enough to guarantee most of the in-sync lookups are in the warm-section. With default Kafka
      settings, 8KB index corresponds to about 4MB (offset index) or 2.7MB (time index) log messages.

   We can't set make N (_warmEntries) to be larger than 8192, as there is no simple way to guarantee all the "warm"
   section pages are really warm (touched in every lookup) on a typical 4KB-page host.

   In there future, we may use a backend thread to periodically touch the entire warm section. So that, we can
   1) support larger warm section
   2) make sure the warm section of low QPS topic-partitions are really warm.
 */
  protected def _warmEntries: Int = 8192 / entrySize

  protected val lock = new ReentrantLock

  @volatile
  protected var mmap: MappedByteBuffer = {
    // mark 这个file都是手动new的File对象实际上并不确定会存在 所以这里实际上是尝试创建一个新的文件对象
    val newlyCreated = file.createNewFile()
    // mark 创建随机读写文件流
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")

    try {
      /* pre-allocate the file if necessary */
      // mark 如果是新建的文件 那么就设置文件大小
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        // mark   roundDownToExactMultiple 向下取整
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file */
      // mark 创建内存映射
      _length = raf.length()
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      // mark 设置游标 如果是新文件则指向文件头否则指向尾部
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      // mark raf只是用来创建mmap的中间对象 创建好之后徐亚关闭
      CoreUtils.swallow(raf.close(), AbstractIndex)
    }
  }

  /**
   * 索引文件最大能够容纳的条目数 mmap的size除8个字节
   */
  @volatile
  private[this] var _maxEntries: Int = mmap.limit() / entrySize

  /** 计算索引文件中条目的数量 文件的最后一位除以8个字节 */
  @volatile
  protected var _entries: Int = mmap.position() / entrySize

  /**
   * 判断当前条目数是否大于最大条目数
   */
  def isFull: Boolean = _entries >= _maxEntries

  def file: File = _file

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

  /**
   * mark 重置内存映射和底层文件的大小。在以下两种情况下使用：
   *
   * 1. 在关闭段或新段滚动时调用的 trimToValidSize() 中使用。
   * 2. 从磁盘加载段或截断回旧段以使新日志段变为活动时使用；我们希望将索引大小重置为最大索引大小，以避免滚动新段。
   *
   * @param newSize 新的索引文件大小。
   * @return 布尔值，指示内存映射和底层文件的大小是否发生了变化。
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      // mark 将新大小调整为条目大小(entry size index为8字节 time为12字节)的整数倍
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      // mark 前后大小相同则返回false
      if (_length == roundedNewSize) {
        debug(s"Index ${file.getAbsolutePath} was not resized because it already has size $roundedNewSize")
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* mark Windows 或 z/OS 不允许我们在文件被内存映射时修改文件长度 :-(  所以必须得先关闭内存映射 */
          if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
            safeForceUnmap()
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          debug(s"Resized ${file.getAbsolutePath} to $roundedNewSize, position is ${mmap.position()} " +
            s"and limit is ${mmap.limit()}")
          true
        } finally {
          CoreUtils.swallow(raf.close(), AbstractIndex)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File): Unit = {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath, false)
    finally _file = f
  }

  /**
   * Flush the data in the index to disk
   */
  def flush(): Unit = {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    closeHandler()
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize(): Unit = {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes: Int = entrySize * _entries

  /** Close the index */
  def close(): Unit = {
    trimToValidSize()
    closeHandler()
  }

  def closeHandler(): Unit = {
    // On JVM, a memory mapping is typically unmapped by garbage collector.
    // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
    // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
    // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
   * @throws IndexOffsetOverflowException
   */
  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  /**
   * Check if a particular offset is valid to be appended to this index.
   * @param offset The offset to check
   * @return true if this offset is valid to be appended to this index; false otherwise
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  protected def safeForceUnmap(): Unit = {
    if (mmap != null) {
      try forceUnmap()
      catch {
        case t: Throwable => error(s"Error unmapping index $file", t)
      }
    }
  }

  /**
   * Forcefully free the buffer's mmap.
   */
  protected[log] def forceUnmap(): Unit = {
    try ByteBufferUnmapper.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows or z/OS. We do this
   * because Windows or z/OS won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n the slot
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *
   * @param idx The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * mark 索引寻址核心逻辑
   * Lookup lower and upper bounds for the given target.
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchType): (Int, Int) = {
    // check if the index is empty
    if(_entries == 0)
      return (-1, -1)

    def binarySearch(begin: Int, end: Int) : (Int, Int) = {
      // binary search for the entry
      var lo = begin
      var hi = end
      while(lo < hi) {
        val mid = (lo + hi + 1) >>> 1
        val found = parseEntry(idx, mid)
        val compareResult = compareIndexEntry(found, target, searchEntity)
        if(compareResult > 0)
          hi = mid - 1
        else if(compareResult < 0)
          lo = mid
        else
          return (mid, mid)
      }
      (lo, if (lo == _entries - 1) -1 else lo + 1)
    }

    val firstHotEntry = Math.max(0, _entries - 1 - _warmEntries)
    // check if the target offset is in the warm section of the index
    if(compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
      return binarySearch(firstHotEntry, _entries - 1)
    }

    // check if the target offset is smaller than the least offset
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    binarySearch(0, firstHotEntry)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchType): Int = {
    searchEntity match {
      case IndexSearchType.KEY => java.lang.Long.compare(indexEntry.indexKey, target)
      case IndexSearchType.VALUE => java.lang.Long.compare(indexEntry.indexValue, target)
    }
  }

  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundDownToExactMultiple(67, 8) == 64
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

}

object AbstractIndex extends Logging {
  override val loggerName: String = classOf[AbstractIndex].getName
}

sealed trait IndexSearchType
object IndexSearchType {
  case object KEY extends IndexSearchType
  case object VALUE extends IndexSearchType
}
