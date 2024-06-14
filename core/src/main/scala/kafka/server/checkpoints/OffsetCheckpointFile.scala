/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server.checkpoints

import kafka.server.LogDirFailureChannel
import kafka.server.epoch.EpochEntry
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.common.CheckpointFile.EntryFormatter

import java.io._
import java.util.Optional
import java.util.regex.Pattern
import scala.collection._

object OffsetCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private[checkpoints] val CurrentVersion = 0

  /**
   * 对 (TopicPartition, Long) 类型的条目进行格式化的对象。这个对象扩展了 EntryFormatter，
   * 并提供了将条目转换为字符串以及从字符串解析条目的方法。
   */
  object Formatter extends EntryFormatter[(TopicPartition, Long)] {

    /**
     * 将 (TopicPartition, Long) 类型的条目转换为字符串。字符串的格式是 "topic partition offset"。
     *
     * @param entry 要转换的条目，包含一个 TopicPartition 和一个偏移量。
     * @return 转换后的字符串，格式为 "topic partition offset"。
     */
    override def toString(entry: (TopicPartition, Long)): String = {
      s"${entry._1.topic} ${entry._1.partition} ${entry._2}"
    }

    /**
     * 从字符串解析出 (TopicPartition, Long) 类型的条目。字符串应该是 "topic partition offset" 的格式。
     *
     * @param line 要解析的字符串，应该包含 topic、partition 和 offset，使用空格分隔。
     * @return 如果解析成功，返回包含 TopicPartition 和偏移量的 Optional；否则返回 Optional.empty()。
     */
    override def fromString(line: String): Optional[(TopicPartition, Long)] = {
      // mark 根据空格分割字符串
      WhiteSpacesPattern.split(line) match {
        case Array(topic, partition, offset) =>
          Optional.of(new TopicPartition(topic, partition.toInt), offset.toLong)
        case _ => Optional.empty()
      }
    }
  }
}

trait OffsetCheckpoint {
  def write(epochs: Seq[EpochEntry]): Unit
  def read(): Seq[EpochEntry]
}

/**
 * mark 此类用来专门处理 recovery-point-offset-checkpoint文件的读写
 * mark recovery-point-offset-checkpoint 是 Kafka 中的一个检查点文件，用于存储各个分区的恢复点偏移量。
 * mark 这个文件在 Kafka 服务器的恢复过程中起到关键作用，帮助 Kafka 确定从哪里开始恢复日志。
 * 此类将 (Partition => Offsets) 的映射持久化到文件中（针对某个副本）
 *
 * 偏移量检查点文件的格式如下：
 * -----检查点文件开始------
 *  0                <- OffsetCheckpointFile.currentVersion
 * 2                <- 后续条目的数量
 * tp1  par1  1     <- 格式为：TOPIC  PARTITION  OFFSET
 *  tp1  par2  2
 * -----检查点文件结束----------
 */
class OffsetCheckpointFile(val file: File, logDirFailureChannel: LogDirFailureChannel = null) {

  // mark 这个是对检查点读写器的包装对象，里面对读写方法进行了异常处理，异常处理由该类执行LogDirFailureChannel
  val checkpoint = new CheckpointFileWithFailureHandler[(TopicPartition, Long)](
    file,
    OffsetCheckpointFile.CurrentVersion,
    OffsetCheckpointFile.Formatter,
    logDirFailureChannel,
    file.getParent
  )

  /**
   * 将给定的偏移量映射写入检查点文件。
   *
   * @param offsets 一个 Map，其中键是 TopicPartition，值是偏移量。
   */
  def write(offsets: Map[TopicPartition, Long]): Unit = checkpoint.write(offsets)

  /**
   * 从检查点文件中读取偏移量。
   *
   * @return 一个 Map，其中键是 TopicPartition，值是偏移量。
   */
  def read(): Map[TopicPartition, Long] = checkpoint.read().toMap
}

trait OffsetCheckpoints {
  def fetch(logDir: String, topicPartition: TopicPartition): Option[Long]
}

/**
 * Loads checkpoint files on demand and caches the offsets for reuse.
 */
class LazyOffsetCheckpoints(checkpointsByLogDir: Map[String, OffsetCheckpointFile]) extends OffsetCheckpoints {
  private val lazyCheckpointsByLogDir = checkpointsByLogDir.map { case (logDir, checkpointFile) =>
    logDir -> new LazyOffsetCheckpointMap(checkpointFile)
  }.toMap

  override def fetch(logDir: String, topicPartition: TopicPartition): Option[Long] = {
    val offsetCheckpointFile = lazyCheckpointsByLogDir.getOrElse(logDir,
      throw new IllegalArgumentException(s"No checkpoint file for log dir $logDir"))
    offsetCheckpointFile.fetch(topicPartition)
  }
}

class LazyOffsetCheckpointMap(checkpoint: OffsetCheckpointFile) {
  private lazy val offsets: Map[TopicPartition, Long] = checkpoint.read()

  def fetch(topicPartition: TopicPartition): Option[Long] = {
    offsets.get(topicPartition)
  }

}
