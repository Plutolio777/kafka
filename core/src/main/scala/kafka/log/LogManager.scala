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

import kafka.log.LogConfig.MessageFormatVersion
import java.io._
import java.nio.file.Files
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.metadata.ConfigRepository
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.common.utils.{KafkaThread, Time, Utils}
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import kafka.utils.Implicits._
import java.util.Properties

import org.apache.kafka.server.common.MetadataVersion

import scala.annotation.nowarn

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 configRepository: ConfigRepository,
                 val initialDefaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 recoveryThreadsPerDataDir: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxTransactionTimeoutMs: Int,
                 val producerStateManagerConfig: ProducerStateManagerConfig,
                 val producerIdExpirationCheckIntervalMs: Int,
                 interBrokerProtocolVersion: MetadataVersion,
                 scheduler: Scheduler,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time,
                 val keepPartitionMetadataFile: Boolean) extends Logging with KafkaMetricsGroup {

  import LogManager._

  val InitialTaskDelayMs = 30 * 1000 // mark 初始任务延时为30s

  private val logCreationOrDeletionLock = new Object // mark 日志创建或者删除使用对象锁
  private val currentLogs = new Pool[TopicPartition, UnifiedLog]() // mark 当前日志对象池
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  private val futureLogs = new Pool[TopicPartition, UnifiedLog]() // mark 在同一个broker上如果用户想要将一个副本从一个日志目录移动到另一个日志目录时会创建
  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  private val logsToBeDeleted = new LinkedBlockingQueue[(UnifiedLog, Long)]() // mark 需要删除的日志对象会放在一个链队列中

  // mark 检查logDirs 确保有可以用的日志文件夹
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  // mark 日志相关的所有配置项
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  // mark 每个日志文件夹分配的恢复线程数
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  /**
   * 这个映射包含了所有正在加载和初始化日志的分区。
   * 在这些分区的日志初始化过程中，如果它们的日志配置被更新，则映射中的对应条目将被设置为 "true"。
   * 这将触发在初始化完成后重新加载配置，以获取最新的配置值。
   * 详见 KAFKA-8813，了解更多关于竞争条件的详细信息。
   *
   * @note 这个变量在测试中是可见的。
   */
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  // mark 获取所有日志文件夹锁
  private val dirLocks = lockLogDirs(liveLogDirs)


  // mark 用于加载日志文件夹中的 recovery-point-offset-checkpoint 文件
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    // mark RecoveryPointCheckpointFile:recovery-point-offset-checkpoint
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap

  // mark 用于加载日志文件夹中的 log-start-offset-checkpoint 文件
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  // mark 首选日志文件夹
  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File]() ++= logDirs
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    logDirsSet
  }

  // A map that stores hadCleanShutdown flag for each log dir.
  // mark 用于存储每个日志文件夹是否正常退出标志
  private val hadCleanShutdownFlags = new ConcurrentHashMap[String, Boolean]()

  // A map that tells whether all logs in a log dir had been loaded or not at startup time.
  // mark 日志文件夹加载完毕标志
  private val loadLogsCompletedFlags = new ConcurrentHashMap[String, Boolean]()

  // mark 日志清理器
  @volatile private var _cleaner: LogCleaner = _

  private[kafka] def cleaner: LogCleaner = _cleaner

  newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)

  for (dir <- logDirs) {
    newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath))
  }

  /**
   * 创建并验证给定的日志目录，这些目录不应在指定的离线目录列表中。具体来说，该方法执行以下操作：
   * <ol>
   * <li> 确保目录列表中没有重复项
   * <li> 如果目录不存在，则创建该目录
   * <li> 检查每个路径是否为可读目录
   * </ol>
   *
   * @param dirs               包含要创建和验证的日志目录的 `Seq[File]` 列表。
   * @param initialOfflineDirs 包含初始离线目录的 `Seq[File]` 列表，这些目录在检查过程中会被忽略。
   * @return 一个包含所有有效日志目录的 `ConcurrentLinkedQueue[File]`。
   * @throws IOException    如果在创建或验证目录时遇到问题，会抛出该异常。
   * @throws KafkaException 如果目录列表中存在重复的目录，会抛出该异常。
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    // mark 创建一个线程安全的队列来存储有效的日志目录。
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    // mark 使用一个可变集合来跟踪目录的规范化路径，确保没有重复的路径。
    val canonicalPaths = mutable.HashSet.empty[String]

    // mark 遍历所有指定的目录。
    for (dir <- dirs) {
      try {
        // mark 如果目录在初始离线目录列表中，抛出异常。
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        // mark 如果目录不存在，尝试创建它。
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          val created = dir.mkdirs()
          // 如果创建失败，抛出异常。
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
          // mark 刷新父目录，确保所有更改都被写入磁盘。(调用了 channel.force方法进行刷盘)
          Utils.flushDir(dir.toPath.toAbsolutePath.normalize.getParent)
        }
        // mark 不为文件夹或者不可读则抛出异常。
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // mark 获取目录的规范化路径，确保没有重复的路径。 (如果 `getCanonicalPath` 抛出异常，我们将该目录标记为离线。)
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")

        // mark 将有效的目录添加到 `liveLogDirs` 队列中。
        liveLogDirs.add(dir)
      } catch {
        // 捕获 `IOException` 异常并将目录标记为离线，同时记录错误信息。
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    // mark 如果没有任何有效的日志目录，记录致命错误并终止代理。
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    // mark 返回所有有效的日志目录。
    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  /**
   * The log directory failure handler. It will stop log cleaning in that directory.
   *
   * @param dir        the absolute path of the log directory
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      def removeOfflineLogs(logs: Pool[TopicPartition, UnifiedLog]): Iterable[TopicPartition] = {
        val offlineTopicPartitions: Iterable[TopicPartition] = logs.collect {
          case (tp, log) if log.parentDir == dir => tp
        }
        offlineTopicPartitions.foreach { topicPartition => {
          val removedLog = removeLogAndMetrics(logs, topicPartition)
          removedLog.foreach {
            log => log.closeHandlers()
          }
        }}

        offlineTopicPartitions
      }

      val offlineCurrentTopicPartitions = removeOfflineLogs(currentLogs)
      val offlineFutureTopicPartitions = removeOfflineLogs(futureLogs)

      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * mark 锁定所有给定的日志文件夹
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        val lock = new FileLock(new File(dir, LockFileName))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: UnifiedLog): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  /*
   * mark topic-partition日志加载。
   *
   * @param logDir 日志目录(topic-partition目录)。
   * @param hadCleanShutdown 日志数据文件夹是否正常关闭。
   * @param recoveryPoints 恢复点映射，其中包含每个主题分区的恢复点。
   * @param logStartOffsets 日志起始偏移量映射，其中包含每个主题分区的起始偏移量。
   * @param defaultConfig 默认日志配置。
   * @param topicConfigOverrides 特定主题的配置覆盖映射。
   * @param numRemainingSegments 剩余日志段数量映射。
   *
   * @return 加载的 UnifiedLog 实例。
   *
   * @throws IllegalStateException 如果发现重复的日志目录。
   */
  private[log] def loadLog(logDir: File, // mark 加载的分区文件夹
                           hadCleanShutdown: Boolean, // mark 主日志文件夹是否正常关闭标志
                           recoveryPoints: Map[TopicPartition, Long], // mark 恢复检查点
                           logStartOffsets: Map[TopicPartition, Long], // 起始偏移量检查点
                           defaultConfig: LogConfig, // mark 默认日志配置
                           topicConfigOverrides: Map[String, LogConfig], // mark topic层级相关配置
                           // mark 剩余待加载segment数量
                           numRemainingSegments: ConcurrentMap[String, Int]): UnifiedLog = {

    // mark 解析日志目录以获取主题分区(根据文件夹名称获得Topic-Partition对象)
    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)

    // mark 获取主题配置，如果没有特定配置则使用默认配置(topicConfigOverrides 是设置在zookeeper里面的)
    val config = topicConfigOverrides.getOrElse(topicPartition.topic, defaultConfig)

    // mark 获取恢复点和起始偏移量
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    // mark 这个里面会进行日志的加载 创建 UnifiedLog 实例
    val log = UnifiedLog(
      dir = logDir, // topic-partition日志目录
      config = config, // topic相关配置
      logStartOffset = logStartOffset, // 日志起始偏移量
      recoveryPoint = logRecoveryPoint, // 恢复点偏移量
      maxTransactionTimeoutMs = maxTransactionTimeoutMs, // 事务超时时间
      producerStateManagerConfig = producerStateManagerConfig, // 生产者状态管理器配置
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs, // 生产者ID过期检查间隔
      scheduler = scheduler, // 调度器
      time = time, // SystemTime 工具类
      brokerTopicStats = brokerTopicStats, // BrokerTopicStats 实例
      logDirFailureChannel = logDirFailureChannel, // 失败处理器
      lastShutdownClean = hadCleanShutdown, // 是否正常关闭
      topicId = None, // 主题ID
      keepPartitionMetadataFile = keepPartitionMetadataFile, // 是否保留分区元数据文件
      numRemainingSegments = numRemainingSegments // 剩余日志段数量
    )

    // mark 检查日志目录是否标记为删除
    if (logDir.getName.endsWith(UnifiedLog.DeleteDirSuffix)) {
      // mark 添加到删除日志记录
      addLogToBeDeleted(log)
    } else {
      // mark 将日志添加到当前日志或未来日志映射中（如果存在重复日志 put的话是会返回之前的key）
      val previous = {
        if (log.isFuture) {
          // mark 添加到未来日志
          this.futureLogs.put(topicPartition, log)
        } else {
          // mark 添加到当前日志
          this.currentLogs.put(topicPartition, log)
        }
      }

      // mark 重复日志处理
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"发现重复的日志目录: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"在 ${log.dir.getAbsolutePath} 和 ${previous.dir.getAbsolutePath} 中都发现了 $topicPartition 的重复日志目录。" +
            s"这可能是因为在代理用未来副本替换当前副本时日志目录故障。通过手动删除该分区的两个目录之一来从故障中恢复代理。" +
            s"建议删除最近已知故障的日志目录中的分区。")
      }
    }

    log
  }

  /**
   * mark 日志恢复线程的工厂类。
   * 此类用于生成具有特定命名规则和属性的线程，以支持在指标中进行日志恢复处理。
   *
   * @param dirPath 目录路径，用于生成唯一的线程名称，表示针对特定目录的日志恢复进程。
   */
  class LogRecoveryThreadFactory(val dirPath: String) extends ThreadFactory {
    /**
     * 线程编号的原子整数，确保线程名称的唯一性。
     */
    val threadNum = new AtomicInteger(0)

    /**
     * 创建一个新的线程。
     *
     * @param runnable 由该线程执行的任务。
     * @return 返回一个配置了特定名称和非守护线程属性的线程。
     */
    override def newThread(runnable: Runnable): Thread = {
      // mark 使用KafkaThread静态类生成非守护线程
      KafkaThread.nonDaemon(logRecoveryThreadName(dirPath, threadNum.getAndIncrement()), runnable)
    }
  }

  // create a unique log recovery thread name for each log dir as the format: prefix-dirPath-threadNum, ex: "log-recovery-/tmp/kafkaLogs-0"
  private def logRecoveryThreadName(dirPath: String, threadNum: Int, prefix: String = "log-recovery"): String = s"$prefix-$dirPath-$threadNum"

  /**
   * mark 减少kafka待加载剩余日志的数量。
   *
   * @param numRemainingLogs 一个包含每个路径剩余日志数量的并发映射。
   * @param path 需要减少剩余日志数量的路径。
   * @return 减少 1 后的剩余日志数量。
   * @throws IllegalArgumentException 如果路径为空。
   */
  private[log] def decNumRemainingLogs(numRemainingLogs: ConcurrentMap[String, Int], path: String): Int = {
    require(path != null, "path 不能为空，以更新剩余日志的指标。")
    numRemainingLogs.compute(path, (_, oldVal) => oldVal - 1)
  }

  /**
   * mark 恢复并加载给定数据目录中的所有日志
   *
   * @param defaultConfig        默认日志配置
   * @param topicConfigOverrides 主题配置覆盖映射
   */
  private[log] def loadLogs(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig]): Unit = {
    info(s"Loading logs from log dirs $liveLogDirs") // mark 加载日志只会针对live dirs
    // mark 记录开始时间戳
    val startMs = time.hiResClockMs()
    // mark 初始化线程池数组 每个数据文件夹都有一个线程池 保存在这个里面
    val threadPools = ArrayBuffer.empty[ExecutorService]
    // mark 初始化离线目录集合（加载日志过程中发现的离线日志文件夹）
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    // mark 初始化恢复线程实例容器
    val jobs = ArrayBuffer.empty[Seq[Future[_]]]
    var numTotalLogs = 0
    // mark 日志文件夹中剩余待加载topic的数量
    val numRemainingLogs: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]
    // mark 日志恢复线程名称到剩余段数量的映射，用于 remainingSegmentsToRecover 度量
    val numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]

    // mark 报告IO异常的方法 （如果发生IC操作增加到offlineDirs中）
    def handleIOException(logDirAbsolutePath: String, e: IOException): Unit = {
      offlineDirs.add((logDirAbsolutePath, e))
      error(s"Error while loading log dir $logDirAbsolutePath", e)
    }

    // mark 开始遍历日志文件夹
    for (dir <- liveLogDirs) {
      // mark 获取日志文件夹绝对路径
      val logDirAbsolutePath = dir.getAbsolutePath
      var hadCleanShutdown: Boolean = false
      try {
        // mark 根据配置 num.recovery.threads.per.data.dir 创建固定大小的线程池 现成工厂 LogRecoveryThreadFactory
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
          new LogRecoveryThreadFactory(logDirAbsolutePath))
        // mark 缓存生成的线程池
        threadPools.append(pool)
        // mark 该文件是用来标志kafka是否为正常关闭 （${日志文件夹}.kafka_cleanshutdown）
        // mark Kafka 服务器在接收到关闭信号时，会执行一系列关闭操作，包括将所有未写入的数据刷到磁盘、关闭所有活动的文件句柄等。
        //  完成这些操作后，会在每个日志目录中创建或更新 .kafka_cleanshutdown 文件。
        val cleanShutdownFile = new File(dir, LogLoader.CleanShutdownFile)
        // mark 如果存在说明是正常关闭kafka
        if (cleanShutdownFile.exists) {
          // mark 如果有该文件则跳过日志恢复的过程
          info(s"Skipping recovery for all logs in $logDirAbsolutePath since clean shutdown file was found")
          // 缓存清洁关闭状态，并将其用于剩余的日志加载工作流。删除 CleanShutdownFile，以便在代理加载日志时崩溃时，
          // 在下次启动期间将其视为硬关闭。KAFKA-10471
          Files.deleteIfExists(cleanShutdownFile.toPath)
          hadCleanShutdown = true
        } else {
          info(s"Attempting recovery for all logs in $logDirAbsolutePath since no clean shutdown file was found")
        }
        // mark 保存日志路径和是否正常关闭标志位
        hadCleanShutdownFlags.put(logDirAbsolutePath, hadCleanShutdown)

        // mark 加载偏移量检查点数据（恢复点偏移量是 Kafka 在崩溃或重启后，可以从磁盘安全恢复的最早偏移量。）
        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          // mark 读取恢复检查点(recovery-point-offset-checkpoint)文件生成 Map<Set(TopicPartition, Offset)> 对象
          recoveryPoints = this.recoveryPointCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
        }

        // mark 加载初始偏移量检查点数据（记录了每个分区的初始偏移量）
        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          // mark 读取恢复检查点(log-start-offset-checkpoint)文件生成 Map<Set(TopicPartition, Offset)> 对象
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
        }


        // mark 加载所有日志文件夹Array(File) UnifiedLog.parseTopicPartitionName(logDir) 会将文件夹转换为 TopicPartition
        //  1.必须为文件夹
        //  2.UnifiedLog.parseTopicPartitionName解析的topic名称不为 __cluster_metadata
        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(logDir =>
          logDir.isDirectory && UnifiedLog.parseTopicPartitionName(logDir).topic != KafkaRaftServer.MetadataTopic)
        // mark 保存日志（分区）总数
        numTotalLogs += logsToLoad.length
        // mark 保存每个文件的日志（分区）总量（待加载）
        numRemainingLogs.put(logDirAbsolutePath, logsToLoad.length)
        // mark 保存日志完成标志位
        loadLogsCompletedFlags.put(logDirAbsolutePath, logsToLoad.isEmpty)

        // mark 为每个topic文件夹生成线程任务（加载日志）
        val jobsForDir = logsToLoad.map { logDir =>
          val runnable: Runnable = () => {
            debug(s"Loading log $logDir")
            var log = None: Option[UnifiedLog]
            val logLoadStartMs = time.hiResClockMs()
            try {
              // mark 加载日志
              log = Some(loadLog(logDir, hadCleanShutdown, recoveryPoints, logStartOffsets,
                defaultConfig, topicConfigOverrides, numRemainingSegments))
            } catch {
              case e: IOException =>
                handleIOException(logDirAbsolutePath, e)
              case e: KafkaStorageException if e.getCause.isInstanceOf[IOException] =>
              // KafkaStorageException 可能会被抛出，例如在写入 LeaderEpochFileCache 时
              // 并且在将 IOException 转换为 KafkaStorageException 时，我们已经处理了异常。因此，我们可以忽略它。
            } finally {
              // mark 记录加载时长
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              // mark 记录剩余待加载topic 日志文件夹数量
              val remainingLogs = decNumRemainingLogs(numRemainingLogs, logDirAbsolutePath)
              val currentNumLoaded = logsToLoad.length - remainingLogs
              // mark 打印加载日志
              log match {
                case Some(loadedLog) => info(s"Completed load of $loadedLog with ${loadedLog.numberOfSegments} segments in ${logLoadDurationMs}ms " +
                  s"($currentNumLoaded/${logsToLoad.length} completed in $logDirAbsolutePath)")
                case None => info(s"Error while loading logs in $logDir in ${logLoadDurationMs}ms ($currentNumLoaded/${logsToLoad.length} completed in $logDirAbsolutePath)")
              }
              // mark 如果全部加载完毕则当前日志文件夹添加加载完毕标志位
              if (remainingLogs == 0) {
                // loadLog 在 logDir 下的所有日志完成后，标记它。
                loadLogsCompletedFlags.put(logDirAbsolutePath, true)
              }
            }
          }
          runnable
        }
        // mark 将Runnable提交到线程池 并将Future添加到jobs用于后续获取结果
        jobs += jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          handleIOException(logDirAbsolutePath, e)
      }
    }

    try {
      // mark 添加日志加载的监控指标
      addLogRecoveryMetrics(numRemainingLogs, numRemainingSegments)
      // mark 等待所有任务执行完毕（没有返回值）
      for (dirJobs <- jobs) {
        dirJobs.foreach(_.get)
      }
      // mark 离线日志文件夹处理
      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while loading log dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      // mark 移除日志恢复指标以及关闭线程池
      removeLogRecoveryMetrics()
      threadPools.foreach(_.shutdown())
    }

    info(s"Loaded $numTotalLogs logs in ${time.hiResClockMs() - startMs}ms.")
  }

  private[log] def addLogRecoveryMetrics(numRemainingLogs: ConcurrentMap[String, Int],
                                         numRemainingSegments: ConcurrentMap[String, Int]): Unit = {
    debug("Adding log recovery metrics")
    for (dir <- logDirs) {
      newGauge("remainingLogsToRecover", () => numRemainingLogs.get(dir.getAbsolutePath),
        Map("dir" -> dir.getAbsolutePath))
      for (i <- 0 until numRecoveryThreadsPerDataDir) {
        val threadName = logRecoveryThreadName(dir.getAbsolutePath, i)
        newGauge("remainingSegmentsToRecover", () => numRemainingSegments.get(threadName),
          Map("dir" -> dir.getAbsolutePath, "threadNum" -> i.toString))
      }
    }
  }

  private[log] def removeLogRecoveryMetrics(): Unit = {
    debug("Removing log recovery metrics")
    for (dir <- logDirs) {
      removeMetric("remainingLogsToRecover", Map("dir" -> dir.getAbsolutePath))
      for (i <- 0 until numRecoveryThreadsPerDataDir) {
        removeMetric("remainingSegmentsToRecover", Map("dir" -> dir.getAbsolutePath, "threadNum" -> i.toString))
      }
    }
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup(topicNames: Set[String]): Unit = {
    // ensure consistency between default config and overrides
    val defaultConfig = currentDefaultConfig
    startupWithConfigOverrides(defaultConfig, fetchTopicConfigOverrides(defaultConfig, topicNames))
  }

  /**
   * mark 获取每个主题相关的配置并覆盖默认配置
   *
   * 该方法用于获取特定主题的配置覆盖，并返回一个映射，其中键是主题名称，值是对应的日志配置。
   * 该方法在测试中可见。
   *
   * @param defaultConfig 默认的日志配置
   * @param topicNames    需要获取配置覆盖的主题名称集合
   * @return 包含主题配置覆盖的映射，其中键是主题名称，值是日志配置
   */
  @nowarn("cat=deprecation")
  private[log] def fetchTopicConfigOverrides(defaultConfig: LogConfig, topicNames: Set[String]): Map[String, LogConfig] = {
    val topicConfigOverrides = mutable.Map[String, LogConfig]()
    // mark 获取默认配置
    val defaultProps = defaultConfig.originals()
    // mark 遍历topic名称
    topicNames.foreach { topicName =>
      // mark 从zookeeper中获取topic的动态配置
      var overrides = configRepository.topicConfig(topicName)
      // 仅为有覆盖的主题包括配置以节省内存
      if (!overrides.isEmpty) {
        Option(overrides.getProperty(LogConfig.MessageFormatVersionProp)).foreach { versionString =>
          val messageFormatVersion = new MessageFormatVersion(versionString, interBrokerProtocolVersion.version)
          if (messageFormatVersion.shouldIgnore) {
            val copy = new Properties()
            copy.putAll(overrides)
            copy.remove(LogConfig.MessageFormatVersionProp)
            overrides = copy

            if (messageFormatVersion.shouldWarn)
              warn(messageFormatVersion.topicWarningMessage(topicName))
          }
        }

        // mark 用overrides覆盖defaultProps中相同配置
        val logConfig = LogConfig.fromProps(defaultProps, overrides)

        topicConfigOverrides(topicName) = logConfig
      }
    }
    // mark 返回topic名称与配置的映射Map
    topicConfigOverrides
  }

  private def fetchLogConfig(topicName: String): LogConfig = {
    // ensure consistency between default config and overrides
    val defaultConfig = currentDefaultConfig
    fetchTopicConfigOverrides(defaultConfig, Set(topicName)).values.headOption.getOrElse(defaultConfig)
  }

  /**
   * mark 启动日志管理器主要逻辑
   *
   * @param defaultConfig        默认日志相关的配置
   * @param topicConfigOverrides topic层级的相关配置映射 Map<TopicName, LogConfig>
   */
  private[log] def startupWithConfigOverrides(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig]): Unit = {
    // mark 加载日志
    loadLogs(defaultConfig, topicConfigOverrides) // this could take a while if shutdown was not clean

    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         deleteLogs _,
                         delay = InitialTaskDelayMs,
                         unit = TimeUnit.MILLISECONDS)
    }
    if (cleanerConfig.enableCleaner) {
      _cleaner = new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
      _cleaner.startup()
    }
  }

  /**
   * Close all the logs
   */
  def shutdown(): Unit = {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    val localLogsByDir = logsByDir

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
        KafkaThread.nonDaemon(s"log-closing-${dir.getAbsolutePath}", _))
      threadPools.append(pool)

      val logs = logsInDir(localLogsByDir, dir).values

      val jobsForDir = logs.map { log =>
        val runnable: Runnable = () => {
          // flush the log to ensure latest possible recovery point
          log.flush(true)
          log.close()
        }
        runnable
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      jobs.forKeyValue { (dir, dirJobs) =>
        if (waitForAllToComplete(dirJobs,
          e => warn(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}"))) {
          val logs = logsInDir(localLogsByDir, dir)

          // update the last flush point
          debug(s"Updating recovery points at $dir")
          checkpointRecoveryOffsetsInDir(dir, logs)

          debug(s"Updating log start offsets at $dir")
          checkpointLogStartOffsetsInDir(dir, logs)

          // mark that the shutdown was clean by creating marker file for log dirs that:
          //  1. had clean shutdown marker file; or
          //  2. had no clean shutdown marker file, but all logs under it have been recovered at startup time
          val logDirAbsolutePath = dir.getAbsolutePath
          if (hadCleanShutdownFlags.getOrDefault(logDirAbsolutePath, false) ||
              loadLogsCompletedFlags.getOrDefault(logDirAbsolutePath, false)) {
            debug(s"Writing clean shutdown marker at $dir")
            CoreUtils.swallow(Files.createFile(new File(dir, LogLoader.CleanShutdownFile).toPath), this)
          }
        }
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    val affectedLogs = ArrayBuffer.empty[UnifiedLog]
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        // May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            affectedLogs += log
          if (needToStopCleaner && !isFuture)
            maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
        } finally {
          if (needToStopCleaner && !isFuture)
            resumeCleaning(topicPartition)
        }
      }
    }

    for (dir <- affectedLogs.map(_.parentDirFile).distinct) {
      checkpointRecoveryOffsetsInDir(dir)
    }
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean): Unit = {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
      // Abort and pause the cleaning of the log, and resume after truncation is done.
      if (!isFuture)
        abortAndPauseCleaning(topicPartition)
      try {
        log.truncateFullyAndStartAt(newOffset)
        if (!isFuture)
          maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
      } finally {
        if (!isFuture)
          resumeCleaning(topicPartition)
      }
      checkpointRecoveryOffsetsInDir(log.parentDirFile)
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
    }
  }

  /**
   * Checkpoint recovery offsets for all the logs in logDir.
   *
   * @param logDir the directory in which the logs to be checkpointed are
   */
  // Only for testing
  private[log] def checkpointRecoveryOffsetsInDir(logDir: File): Unit = {
    checkpointRecoveryOffsetsInDir(logDir, logsInDir(logDir))
  }

  /**
   * Checkpoint recovery offsets for all the provided logs.
   *
   * @param logDir the directory in which the logs are
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointRecoveryOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      recoveryPointCheckpoints.get(logDir).foreach { checkpoint =>
        val recoveryOffsets = logsToCheckpoint.map { case (tp, log) => tp -> log.recoveryPoint }
        // checkpoint.write calls Utils.atomicMoveWithFallback, which flushes the parent
        // directory and guarantees crash consistency.
        checkpoint.write(recoveryOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}")
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath,
          s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}", e)
    }
  }

  /**
   * Checkpoint log start offsets for all the provided logs in the provided directory.
   *
   * @param logDir the directory in which logs are checkpointed
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointLogStartOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      logStartOffsetCheckpoints.get(logDir).foreach { checkpoint =>
        val logStartOffsets = logsToCheckpoint.collect {
          case (tp, log) if log.logStartOffset > log.logSegments.head.baseOffset => tp -> log.logStartOffset
        }
        checkpoint.write(logStartOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing log start offsets checkpoint in directory $logDir: ${e.getMessage}")
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * Abort and pause cleaning of the provided partition and log a message about it.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortAndPauseCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted and paused")
    }
  }

  /**
   * Abort cleaning of the provided partition and log a message about it.
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted")
    }
  }

  /**
   * Resume cleaning of the provided partition and log a message about it.
   */
  private def resumeCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.resumeCleaning(Seq(topicPartition))
      info(s"Cleaning for partition $topicPartition is resumed")
    }
  }

  /**
   * Truncate the cleaner's checkpoint to the based offset of the active segment of
   * the provided log.
   */
  private def maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log: UnifiedLog, topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[UnifiedLog] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * Method to indicate that logs are getting initialized for the partition passed in as argument.
   * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
   * initialization is done.
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    partitionsInitializing(topicPartition) = false
  }

  /**
   * Mark the partition configuration for all partitions that are getting initialized for topic
   * as dirty. That will result in reloading of configuration once initialization is done.
   */
  def topicConfigUpdated(topic: String): Unit = {
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Update the configuration of the provided topic.
   */
  def updateTopicConfig(topic: String,
                        newTopicConfig: Properties): Unit = {
    topicConfigUpdated(topic)
    val logs = logsByTopic(topic)
    if (logs.nonEmpty) {
      // Combine the default properties with the overrides in zk to create the new LogConfig
      val newLogConfig = LogConfig.fromProps(currentDefaultConfig.originals, newTopicConfig)
      logs.foreach { log =>
        val oldLogConfig = log.updateConfig(newLogConfig)
        if (oldLogConfig.compact && !newLogConfig.compact) {
          abortCleaning(log.topicPartition)
        }
      }
    }
  }

  /**
   * Mark all in progress partitions having dirty configuration if broker configuration is updated.
   */
  def brokerConfigUpdated(): Unit = {
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Method to indicate that the log initialization for the partition passed in as argument is
   * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
   *
   * It will retrieve the topic configs a second time if they were updated while the
   * relevant log was being loaded.
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[UnifiedLog]): Unit = {
    val removedValue = partitionsInitializing.remove(topicPartition)
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig(topicPartition.topic)))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True if the future log of the specified partition should be returned or created
   * @param topicId The topic ID of the partition's topic
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   * @throws InconsistentTopicIdException if the topic ID in the log does not match the topic ID provided
   */
  def getOrCreateLog(topicPartition: TopicPartition, isNew: Boolean = false, isFuture: Boolean = false, topicId: Option[Uuid]): UnifiedLog = {
    logCreationOrDeletionLock synchronized {
      val log = getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDirs: List[File] = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
            nextLogDirs()
        }

        val logDirName = {
          if (isFuture)
            UnifiedLog.logFutureDirName(topicPartition)
          else
            UnifiedLog.logDirName(topicPartition)
        }

        val logDir = logDirs
          .iterator // to prevent actually mapping the whole list, lazy map
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // If Failure, will throw

        val config = fetchLogConfig(topicPartition.topic)
        val log = UnifiedLog(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxTransactionTimeoutMs = maxTransactionTimeoutMs,
          producerStateManagerConfig = producerStateManagerConfig,
          producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel,
          topicId = topicId,
          keepPartitionMetadataFile = keepPartitionMetadataFile)

        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        info(s"Created log for partition $topicPartition in $logDir with properties ${config.overriddenConfigsAsLoggableString}")
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition)

        log
      }
      // When running a ZK controller, we may get a log that does not have a topic ID. Assign it here.
      if (log.topicId.isEmpty) {
        topicId.foreach(log.assignTopicId)
      }

      // Ensure topic IDs are consistent
      topicId.foreach { topicId =>
        log.topicId.foreach { logTopicId =>
          if (topicId != logTopicId)
            throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
              s"but log already contained topic ID $logTopicId")
        }
      }
      log
    }
  }

  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    val logDirPath = logDir.getAbsolutePath
    if (isLogDirOnline(logDirPath)) {
      val dir = new File(logDirPath, logDirName)
      try {
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          warn(msg, e)
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    val fileDeleteDelayMs = currentDefaultConfig.fileDeleteDelayMs
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + fileDeleteDelayMs - time.milliseconds()
        } else
          fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(UnifiedLog.logDirName(topicPartition), true)
      destLog.updateHighWatermark(sourceLog.highWatermark)

      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.parentDirFile, destLog.parentDirFile)
        resumeCleaning(topicPartition)
      }

      try {
        sourceLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), true)
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        val logDir = sourceLog.parentDirFile
        val logsToCheckpoint = logsInDir(logDir)
        checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
        checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        sourceLog.removeLogMetrics()
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @param checkpoint True if checkpoints must be written
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition,
                  isFuture: Boolean = false,
                  checkpoint: Boolean = true): Option[UnifiedLog] = {
    val removedLog: Option[UnifiedLog] = logCreationOrDeletionLock synchronized {
      removeLogAndMetrics(if (isFuture) futureLogs else currentLogs, topicPartition)
    }
    removedLog match {
      case Some(removedLog) =>
        // We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
        if (cleaner != null && !isFuture) {
          cleaner.abortCleaning(topicPartition)
          if (checkpoint) {
            cleaner.updateCheckpoints(removedLog.parentDirFile, partitionToRemove = Option(topicPartition))
          }
        }
        removedLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), false)
        if (checkpoint) {
          val logDir = removedLog.parentDirFile
          val logsToCheckpoint = logsInDir(logDir)
          checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
          checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        }
        addLogToBeDeleted(removedLog)
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")

      case None =>
        if (offlineLogDirs.nonEmpty) {
          throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
        }
    }

    removedLog
  }

  /**
   * Rename the directories of the given topic-partitions and add them in the queue for
   * deletion. Checkpoints are updated once all the directories have been renamed.
   *
   * @param topicPartitions The set of topic-partitions to delete asynchronously
   * @param errorHandler The error handler that will be called when a exception for a particular
   *                     topic-partition is raised
   */
  def asyncDelete(topicPartitions: Set[TopicPartition],
                  errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    val logDirs = mutable.Set.empty[File]

    topicPartitions.foreach { topicPartition =>
      try {
        getLog(topicPartition).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, checkpoint = false)
        }
        getLog(topicPartition, isFuture = true).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, isFuture = true, checkpoint = false)
        }
      } catch {
        case e: Throwable => errorHandler(topicPartition, e)
      }
    }

    val logsByDirCached = logsByDir
    logDirs.foreach { logDir =>
      if (cleaner != null) cleaner.updateCheckpoints(logDir)
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
      checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Provides the full ordered list of suggested directories for the next partition.
   * Currently this is done by calculating the number of partitions in each directory and then sorting the
   * data directories by fewest partitions.
   */
  private def nextLogDirs(): List[File] = {
    if(_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  def cleanupLogs(): Unit = {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds

    // clean current logs.
    val deletableLogs = {
      if (cleaner != null) {
        // prevent cleaner from working on same partitions when changing cleanup policy
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }

    try {
      deletableLogs.foreach {
        case (topicPartition, log) =>
          debug(s"Garbage collecting '${log.name}'")
          total += log.deleteOldSegments()

          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            // clean future logs
            debug(s"Garbage collecting future log '${futureLog.name}'")
            total += futureLog.deleteOldSegments()
          }
      }
    } finally {
      if (cleaner != null) {
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"Log cleanup completed. $total files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[UnifiedLog] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[UnifiedLog] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, UnifiedLog]] = {
    // This code is called often by checkpoint processes and is written in a way that reduces
    // allocations and CPU with many topic partitions.
    // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, UnifiedLog]]()
    def addToDir(tp: TopicPartition, log: UnifiedLog): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, UnifiedLog]()).put(tp, log)
    }
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  private def logsInDir(dir: File): Map[TopicPartition, UnifiedLog] = {
    logsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  private def logsInDir(cachedLogsByDir: Map[String, Map[TopicPartition, UnifiedLog]],
                        dir: File): Map[TopicPartition, UnifiedLog] = {
    cachedLogsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
              s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush(false)
      } catch {
        case e: Throwable =>
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }

  private def removeLogAndMetrics(logs: Pool[TopicPartition, UnifiedLog], tp: TopicPartition): Option[UnifiedLog] = {
    val removedLog = logs.remove(tp)
    if (removedLog != null) {
      removedLog.removeLogMetrics()
      Some(removedLog)
    } else {
      None
    }
  }
}

object LogManager {
  val LockFileName = ".lock"

  /**
   * Wait all jobs to complete
   * @param jobs jobs
   * @param callback this will be called to handle the exception caused by each Future#get
   * @return true if all pass. Otherwise, false
   */
  private[log] def waitForAllToComplete(jobs: Seq[Future[_]], callback: Throwable => Unit): Boolean = {
    jobs.count(future => Try(future.get) match {
      case Success(_) => false
      case Failure(e) =>
        callback(e)
        true
    }) == 0
  }

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"

  /**
   * 创建并配置 LogManager 实例
   * 该方法使用提供的配置和参数来创建一个新的 LogManager 实例，并进行必要的配置。
   *
   * @param config                    Kafka 配置对象
   * @param initialOfflineDirs        初始离线目录列表
   * @param configRepository          配置仓库
   * @param kafkaScheduler            Kafka 调度器
   * @param time                      时间对象
   * @param brokerTopicStats          代理主题统计对象
   * @param logDirFailureChannel      日志目录故障通道
   * @param keepPartitionMetadataFile 是否保留分区元数据文件
   * @return 配置好的 LogManager 实例
   */
  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            configRepository: ConfigRepository,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel,
            keepPartitionMetadataFile: Boolean): LogManager = {

    // mark 从配置中提取与日志相关的配置子集
    val defaultProps = LogConfig.extractLogConfigMap(config)
    // mark 对配置进行相关的验证
    LogConfig.validateValues(defaultProps)
    // mark 生成LogConfig对象（先server.properties文件中的配置作为默认配置）
    val defaultLogConfig = LogConfig(defaultProps)

    // mark 获取日志清理器配置
    val cleanerConfig = LogCleaner.cleanerConfig(config)

    // mark 创建并返回 LogManager 实例
    new LogManager(
      logDirs = config.logDirs.map(new File(_).getAbsoluteFile), // 日志文件夹绝对路径
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile), // 离线文件夹绝对路径
      configRepository = configRepository, // zookeeper配置仓库
      initialDefaultConfig = defaultLogConfig, // LogConfig日志配置对象
      cleanerConfig = cleanerConfig, // 日志清理器配置
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir, // 每个日志文件夹恢复线程数
      flushCheckMs = config.logFlushSchedulerIntervalMs, // 日志刷新调度时间间隔
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs, // 偏移量检查点刷新间隔
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs, // 起始偏移量检查点刷新间隔
      retentionCheckMs = config.logCleanupIntervalMs, // 日志清理时间间隔
      maxTransactionTimeoutMs = config.transactionMaxTimeoutMs, // 事务最大超时时间
      producerStateManagerConfig = new ProducerStateManagerConfig(config.producerIdExpirationMs), // 生产者状态管理配置
      producerIdExpirationCheckIntervalMs = config.producerIdExpirationCheckIntervalMs, // 生产者状态过期时间间隔
      scheduler = kafkaScheduler, // kafka调度器
      brokerTopicStats = brokerTopicStats, // topic状态管理器
      logDirFailureChannel = logDirFailureChannel, // 日志文件夹加载失败处理器
      time = time, // 实践操作工具类
      keepPartitionMetadataFile = keepPartitionMetadataFile, // ?
      interBrokerProtocolVersion = config.interBrokerProtocolVersion
    )
  }

}
