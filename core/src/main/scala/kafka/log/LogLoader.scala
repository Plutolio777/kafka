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

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import kafka.common.LogSegmentOffsetOverflowException
import kafka.log.UnifiedLog.{CleanedFileSuffix, DeletedFileSuffix, SwapFileSuffix, isIndexFile, isLogFile, offsetFromFile}
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.utils.{CoreUtils, Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.snapshot.Snapshots

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.{Set, mutable}

case class LoadedLogOffsets(logStartOffset: Long,
                            recoveryPoint: Long,
                            nextOffsetMetadata: LogOffsetMetadata)

object LogLoader extends Logging {

  /**
   * Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"
}


/**
 * @param dir The directory from which log segments need to be loaded
 * @param topicPartition The topic partition associated with the log being loaded
 * @param config The configuration settings for the log being loaded
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle log
 *                             directory failure
 * @param hadCleanShutdown Boolean flag to indicate whether the associated log previously had a
 *                         clean shutdown
 * @param segments The LogSegments instance into which segments recovered from disk will be
 *                 populated
 * @param logStartOffsetCheckpoint The checkpoint of the log start offset
 * @param recoveryPointCheckpoint The checkpoint of the offset at which to begin the recovery
 * @param leaderEpochCache An optional LeaderEpochFileCache instance to be updated during recovery
 * @param producerStateManager The ProducerStateManager instance to be updated during recovery
 * @param numRemainingSegments The remaining segments to be recovered in this log keyed by recovery thread name
 */
class LogLoader(
  dir: File,
  topicPartition: TopicPartition,
  config: LogConfig,
  scheduler: Scheduler,
  time: Time,
  logDirFailureChannel: LogDirFailureChannel,
  hadCleanShutdown: Boolean,
  segments: LogSegments,
  logStartOffsetCheckpoint: Long,
  recoveryPointCheckpoint: Long,
  leaderEpochCache: Option[LeaderEpochFileCache],
  producerStateManager: ProducerStateManager,
  numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]
) extends Logging {
  logIdent = s"[LogLoader partition=$topicPartition, dir=${dir.getParent}] "

  /**
   * Load the log segments from the log files on disk, and returns the components of the loaded log.
   * Additionally, it also suitably updates the provided LeaderEpochFileCache and ProducerStateManager
   * to reflect the contents of the loaded log.
   *
   * In the context of the calling thread, this function does not need to convert IOException to
   * KafkaStorageException because it is only called before all logs are loaded.
   *
   * @return the offsets of the Log successfully loaded from disk
   *
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
   *                                           overflow index offset
   */
  def load(): LoadedLogOffsets = {
    // mark 1.遍历日志目录中的文件并删除所有临时文件
    // First pass: through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    // mark 清理临时日志段（这里需要注意由于存在swap文件，有的swap文件是需要进行恢复的这里判断哪些swap是有效的）
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // The remaining valid swap files must come from compaction or segment split operation. We can
    // simply rename them to regular segment files. But, before renaming, we should figure out which
    // segments are compacted/split and delete these segment files: this is done by calculating
    // min/maxSwapFileOffset.
    // We store segments that require renaming in this code block, and do the actual renaming later.
    var minSwapFileOffset = Long.MaxValue
    var maxSwapFileOffset = Long.MinValue
    // mark 计算swap文件的最大最小偏移量
    swapFiles.filter(f => UnifiedLog.isLogFile(new File(CoreUtils.replaceSuffix(f.getPath, SwapFileSuffix, "")))).foreach { f =>
      val baseOffset = offsetFromFile(f)
      val segment = LogSegment.open(f.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = UnifiedLog.SwapFileSuffix)
      info(s"Found log file ${f.getPath} from interrupted swap operation, which is recoverable from ${UnifiedLog.SwapFileSuffix} files by renaming.")
      minSwapFileOffset = Math.min(segment.baseOffset, minSwapFileOffset)
      maxSwapFileOffset = Math.max(segment.readNextOffset, maxSwapFileOffset)
    }

    // Second pass: delete segments that are between minSwapFileOffset and maxSwapFileOffset. As
    // discussed above, these segments were compacted or split but haven't been renamed to .delete
    // before shutting down the broker.
    // mark 将偏移量位于 [minSwapFileOffset, maxSwapFileOffset) 之间的日志段删除（标记为delete后缀文件）
    for (file <- dir.listFiles if file.isFile) {
      try {
        if (!file.getName.endsWith(SwapFileSuffix)) {
          val offset = offsetFromFile(file)
          if (offset >= minSwapFileOffset && offset < maxSwapFileOffset) {
            info(s"Deleting segment files ${file.getName} that is compacted but has not been deleted yet.")
            file.delete()
          }
        }
      } catch {
        // offsetFromFile with files that do not include an offset in the file name
        case _: StringIndexOutOfBoundsException =>
        case _: NumberFormatException =>
      }
    }

    // Third pass: rename all swap files.
    // mark 重命名所有swap文件将其转换为正常日志文件
    for (file <- dir.listFiles if file.isFile) {
      if (file.getName.endsWith(SwapFileSuffix)) {
        info(s"Recovering file ${file.getName} by renaming from ${UnifiedLog.SwapFileSuffix} files.")
        file.renameTo(new File(CoreUtils.replaceSuffix(file.getPath, UnifiedLog.SwapFileSuffix, "")))
      }
    }

    // Fourth pass: load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    // mark 加载日志段以及索引文件（这一步会创建LogSegment并且添加到跳表中）
    retryOnOffsetOverflow(() => {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      // mark 先关闭所有的segment
      segments.close()
      // mark 然后清空跳表
      segments.clear()
      // mark 开始加载日志段
      loadSegmentFiles()
    })

    val (newRecoveryPoint: Long, nextOffset: Long) = {

      if (!dir.getAbsolutePath.endsWith(UnifiedLog.DeleteDirSuffix)) {
        val (newRecoveryPoint, nextOffset) = retryOnOffsetOverflow(recoverLog)

        // reset the index size of the currently active log segment to allow more entries
        segments.lastSegment.get.resizeIndexes(config.maxIndexSize)
        (newRecoveryPoint, nextOffset)
      }
      else {
        // mark 如果日志段为空则必须打开一个新的segment来接收新的消息
        if (segments.isEmpty) {
          segments.add(
            LogSegment.open(
              dir = dir,
              baseOffset = 0,
              config,
              time = time,
              initFileSize = config.initFileSize))
        }
        (0L, 0L)
      }
    }

    leaderEpochCache.foreach(_.truncateFromEnd(nextOffset))
    val newLogStartOffset = math.max(logStartOffsetCheckpoint, segments.firstSegment.get.baseOffset)
    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffsetCheckpoint))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    if (!producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")

    // Reload all snapshots into the ProducerStateManager cache, the intermediate ProducerStateManager used
    // during log recovery may have deleted some files without the LogLoader.producerStateManager instance witnessing the
    // deletion.
    producerStateManager.removeStraySnapshots(segments.baseOffsets.toSeq)
    UnifiedLog.rebuildProducerState(
      producerStateManager,
      segments,
      newLogStartOffset,
      nextOffset,
      config.recordVersion,
      time,
      reloadFromCleanShutdown = hadCleanShutdown,
      logIdent)
    val activeSegment = segments.lastSegment.get
    LoadedLogOffsets(
      newLogStartOffset,
      newRecoveryPoint,
      LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size))
  }

  /**
   * 移除日志目录中的任何临时文件，并创建所有可以替换现有段的 .swap 文件列表。
   * 对于日志拆分，我们知道任何基础偏移量高于最小偏移量 .clean 文件的 .swap 文件可能是未完成的拆分操作的一部分。
   * 这种 .swap 文件也会被此方法删除。
   *
   * @return 可以作为段文件和索引文件有效交换的 .swap 文件集
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    // 可变集合，用于存储 swap 和 cleaned 文件
    val swapFiles = mutable.Set[File]()
    val cleanedFiles = mutable.Set[File]()
    // 变量，用于存储 cleaned 文件中的最小偏移量
    var minCleanedFileOffset = Long.MaxValue

    // 遍历目录中的所有文件
    for (file <- dir.listFiles if file.isFile) {
      // 如果文件不可读，抛出异常
      if (!file.canRead)
        throw new IOException(s"无法读取文件 $file")
      val filename = file.getName

      // 删除标记为删除的零散文件，但跳过 KRaft 快照。
      // 这些文件在 `KafkaMetadataLog` 的恢复逻辑中处理。
      // 如果是 delete 文件，则直接删除
      if (filename.endsWith(DeletedFileSuffix) && !filename.endsWith(Snapshots.DELETE_SUFFIX)) {
        debug(s"删除零散的临时文件 ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
        // 如果是 cleaned 文件，保存已清理文件中最小的 offset
      } else if (filename.endsWith(CleanedFileSuffix)) {
        minCleanedFileOffset = Math.min(offsetFromFile(file), minCleanedFileOffset)
        cleanedFiles += file
        // 如果是 swap 文件，则保存进行后续处理
      } else if (filename.endsWith(SwapFileSuffix)) {
        swapFiles += file
      }
    }

    // mark KAFKA-6264: 删除所有基础偏移量大于最小 .cleaned 段偏移量的 .swap 文件。
    // 这些 .swap 文件可能是未完成的拆分操作的一部分。请参阅 Log#splitOverflowedSegment 了解拆分操作的更多详细信息。
    // 以最小已清理文件偏移量为界，区分有效 swap 文件以及无效 swap 文件
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      // 删除无效 swap 文件
      debug(s"删除无效的 swap 文件 ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      Files.deleteIfExists(file.toPath)
    }

    // 既然已经删除了所有构成未完成拆分操作的 .swap 文件，现在删除所有 .clean 文件
    cleanedFiles.foreach { file =>
      // 删除 cleaned 文件
      debug(s"删除零散的 .clean 文件 ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }
    // 返回有效 swap 文件
    validSwapFiles
  }

  /**
   * 当执行过程中抛出 LogSegmentOffsetOverflowException 时，重试提供的函数。
   * 在每次重试之前，溢出的日志段将被拆分为一个或多个段，以确保其中没有偏移溢出。
   *
   * @param fn 要执行的函数
   * @return 如果成功，返回函数的返回值
   * @throws Exception 如果执行的函数抛出 LogSegmentOffsetOverflowException 以外的任何异常，
   *                   则向调用者抛出相同的异常
   */
  private def retryOnOffsetOverflow[T](fn: () => T): T = {
    while (true) {
      try {
        // 尝试执行提供的函数
        return fn()
      } catch {
        // mark 如果出现偏移量溢出异常
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          // mark 拆分溢出的日志段
          val result = UnifiedLog.splitOverflowedSegment(
            e.segment,
            segments,
            dir,
            topicPartition,
            config,
            scheduler,
            logDirFailureChannel,
            logIdent)
          // mark 异步删除生产者快照
          deleteProducerSnapshotsAsync(result.deletedSegments)
      }
    }
    // 如果循环意外退出，抛出非法状态异常
    throw new IllegalStateException()
  }

  /**
   * 从磁盘加载日志段到提供的 `params.segments` 中。
   *
   * 由于此方法仅在所有日志加载之前调用，因此不需要将 IOException 转换为 KafkaStorageException。
   * 可能会遇到具有索引偏移溢出的段，在这种情况下会抛出 LogSegmentOffsetOverflowException 异常。
   * 请注意，在遇到异常之前打开的任何段将保持打开状态，调用者有责任在需要时适当地关闭它们。
   *
   * @throws LogSegmentOffsetOverflowException 如果日志目录包含消息偏移量溢出的段
   */
  private def loadSegmentFiles(): Unit = {
    // mark 以升序加载日志段，因为一个段中的事务数据可能依赖于它之前的段
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      // mark 索引文件处理
      if (isIndexFile(file)) {
        // mark 根据索引的基础偏移量获取对应的log文件File对象
        val offset = offsetFromFile(file)
        // mark 根据offset获取对应的.log日志文件
        val logFile = UnifiedLog.logFile(dir, offset)
        // mark 如果没有对应的log文件中删除
        if (!logFile.exists) {
          warn(s"发现一个孤立的索引文件 ${file.getAbsolutePath}，没有对应的日志文件。")
          Files.deleteIfExists(file.toPath)
        }
        // mark 日志段文件处理
      } else if (isLogFile(file)) {
        // mark 获取基础偏移量
        val baseOffset = offsetFromFile(file)
        // mark 时间索引文件是否存在
        val timeIndexFileNewlyCreated = !UnifiedLog.timeIndexFile(dir, baseOffset).exists()

        // mark 打开日志文件生成segment
        val segment = LogSegment.open(
          dir = dir, // 日志文件所在目录
          baseOffset = baseOffset, // 日志文件基础偏移量
          config, // 日志配置
          time = time, // 时间工具类
          fileAlreadyExists = true)

        // mark 检查segment的完整性 如果有问题则恢复索引
        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            if (hadCleanShutdown || segment.baseOffset < recoveryPointCheckpoint)
              error(s"找不到对应于日志文件 ${segment.log.file.getAbsolutePath} 的偏移索引文件，恢复段并重建索引文件...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"发现一个损坏的索引文件，对应于日志文件 ${segment.log.file.getAbsolutePath}，由于 ${e.getMessage}，恢复段并重建索引文件...")
            recoverSegment(segment)
        }
        // mark 记载完成后添加到segments（跳表）中
        segments.add(segment)
      }
    }
  }

  /**
   * Just recovers the given segment, without adding it to the provided params.segments.
   *
   * @param segment Segment to recover
   *
   * @return The number of bytes truncated from the segment
   *
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */
  private def recoverSegment(segment: LogSegment): Int = {
    val producerStateManager = new ProducerStateManager(
      topicPartition,
      dir,
      this.producerStateManager.maxTransactionTimeoutMs,
      this.producerStateManager.producerStateManagerConfig,
      time)
    UnifiedLog.rebuildProducerState(
      producerStateManager, // 生产者状态管理器
      segments, // 日志段集合
      logStartOffsetCheckpoint, // 日志开始偏移
      segment.baseOffset, //  待恢复日志段基础偏移量
      config.recordVersion, // 记录版本
      time, // 时间工具类
      reloadFromCleanShutdown = false, // 是否重载
      logIdent) // 日志标识符
    val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    producerStateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
   * active segment, and returns the updated recovery point and next offset after recovery. Along
   * the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager inside
   * the provided LogComponents.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only
   * called before all logs are loaded.
   *
   * @return a tuple containing (newRecoveryPoint, nextOffset).
   *
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private[log] def recoverLog(): (Long, Long) = {
    /** return the log end offset if valid */
    def deleteSegmentsIfLogStartGreaterThanLogEnd(): Option[Long] = {
      if (segments.nonEmpty) {
        val logEndOffset = segments.lastSegment.get.readNextOffset
        if (logEndOffset >= logStartOffsetCheckpoint)
          Some(logEndOffset)
        else {
          warn(s"Deleting all segments because logEndOffset ($logEndOffset) " +
            s"is smaller than logStartOffset $logStartOffsetCheckpoint. " +
            "This could happen if segment files were deleted from the file system.")
          removeAndDeleteSegmentsAsync(segments.values)
          leaderEpochCache.foreach(_.clearAndFlush())
          producerStateManager.truncateFullyAndStartAt(logStartOffsetCheckpoint)
          None
        }
      } else None
    }

    // If we have the clean shutdown marker, skip recovery.
    if (!hadCleanShutdown) {
      // mark 根据恢复点获取恢复点之后的segment 返回一个迭代器
      val unflushed = segments.values(recoveryPointCheckpoint, Long.MaxValue)
      val numUnflushed = unflushed.size
      val unflushedIter = unflushed.iterator
      var truncated = false
      var numFlushed = 0
      val threadName = Thread.currentThread().getName
      numRemainingSegments.put(threadName, numUnflushed)

      while (unflushedIter.hasNext && !truncated) {
        val segment = unflushedIter.next()
        info(s"Recovering unflushed segment ${segment.baseOffset}. $numFlushed/$numUnflushed recovered for $topicPartition.")

        val truncatedBytes =
          try {
            // mark 恢复segment
            recoverSegment(segment)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn(s"Found invalid offset during recovery. Deleting the" +
                s" corrupt segment and creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        // mark 如果发生阶截断则删除所有剩余的日志不进行恢复了
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}," +
            s" truncating to offset ${segment.readNextOffset}")
          removeAndDeleteSegmentsAsync(unflushedIter.toList)
          truncated = true
          // segment is truncated, so set remaining segments to 0
          numRemainingSegments.put(threadName, 0)
        } else {
          numFlushed += 1
          numRemainingSegments.put(threadName, numUnflushed - numFlushed)
        }
      }
    }
    // mark 如果恢复点大于日志结束点则删除所有日志
    val logEndOffsetOption = deleteSegmentsIfLogStartGreaterThanLogEnd()

    // mark 如果不存在日志则创建一个新的段
    if (segments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      segments.add(
        LogSegment.open(
          dir = dir,
          baseOffset = logStartOffsetCheckpoint,
          config,
          time = time,
          initFileSize = config.initFileSize,
          preallocate = config.preallocate))
    }

    // Update the recovery point if there was a clean shutdown and did not perform any changes to
    // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
    // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
    // the recovery point when the log is flushed. If we advanced the recovery point here, we could
    // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
    // point and before we flush the segment.
    (hadCleanShutdown, logEndOffsetOption) match {
      case (true, Some(logEndOffset)) =>
        (logEndOffset, logEndOffset)
      case _ =>
        val logEndOffset = logEndOffsetOption.getOrElse(segments.lastSegment.get.readNextOffset)
        (Math.min(recoveryPointCheckpoint, logEndOffset), logEndOffset)
    }
  }

  /**
   * This method deletes the given log segments and the associated producer snapshots, by doing the
   * following for each of them:
   *  - It removes the segment from the segment map so that it will no longer be used for reads.
   *  - It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
   *    synchronization and without the possibility of physically deleting a file while it is being
   *    read.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either
   * called before all logs are loaded or the immediate caller will catch and handle IOException
   *
   * @param segmentsToDelete The log segments to schedule for deletion
   */
  private def removeAndDeleteSegmentsAsync(segmentsToDelete: Iterable[LogSegment]): Unit = {
    if (segmentsToDelete.nonEmpty) {
      // Most callers hold an iterator into the `params.segments` collection and
      // `removeAndDeleteSegmentAsync` mutates it by removing the deleted segment. Therefore,
      // we should force materialization of the iterator here, so that results of the iteration
      // remain valid and deterministic. We should also pass only the materialized view of the
      // iterator to the logic that deletes the segments.
      val toDelete = segmentsToDelete.toList
      info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
      // mark 从segments跳表中删除
      toDelete.foreach { segment =>
        segments.remove(segment.baseOffset)
      }
      // mark 删除日志文件
      UnifiedLog.deleteSegmentFiles(
        toDelete,
        asyncDelete = true,
        dir,
        topicPartition,
        config,
        scheduler,
        logDirFailureChannel,
        logIdent)
      // mark 删除生产者状态快照
      deleteProducerSnapshotsAsync(segmentsToDelete)
    }
  }

  private def deleteProducerSnapshotsAsync(segments: Iterable[LogSegment]): Unit = {
    UnifiedLog.deleteProducerSnapshots(segments,
      producerStateManager,
      asyncDelete = true,
      scheduler,
      config,
      logDirFailureChannel,
      dir.getParent,
      topicPartition)
  }
}
