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

package kafka.server

import java.io._
import java.nio.file.{Files, NoSuchFileException}
import java.util.Properties

import kafka.common.InconsistentBrokerMetadataException
import kafka.server.RawMetaProperties._
import kafka.utils._
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object RawMetaProperties {
  val ClusterIdKey = "cluster.id"
  val BrokerIdKey = "broker.id"
  val NodeIdKey = "node.id"
  val VersionKey = "version"
}

class RawMetaProperties(val props: Properties = new Properties()) {

  def clusterId: Option[String] = {
    Option(props.getProperty(ClusterIdKey))
  }

  def clusterId_=(id: String): Unit = {
    props.setProperty(ClusterIdKey, id)
  }

  def brokerId: Option[Int] = {
    intValue(BrokerIdKey)
  }

  def brokerId_=(id: Int): Unit = {
    props.setProperty(BrokerIdKey, id.toString)
  }

  def nodeId: Option[Int] = {
    intValue(NodeIdKey)
  }

  def nodeId_=(id: Int): Unit = {
    props.setProperty(NodeIdKey, id.toString)
  }

  def version: Int = {
    intValue(VersionKey).getOrElse(0)
  }

  def version_=(ver: Int): Unit = {
    props.setProperty(VersionKey, ver.toString)
  }

  def requireVersion(expectedVersion: Int): Unit = {
    if (version != expectedVersion) {
      throw new RuntimeException(s"Expected version $expectedVersion, but got "+
        s"version $version")
    }
  }

  private def intValue(key: String): Option[Int] = {
    try {
      Option(props.getProperty(key)).map(Integer.parseInt)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Failed to parse $key property " +
        s"as an int: ${e.getMessage}")
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: RawMetaProperties => props.equals(other.props)
    case _ => false
  }

  override def hashCode(): Int = props.hashCode

  override def toString: String = {
    "{" + props.keySet().asScala.toList.asInstanceOf[List[String]].sorted.map {
      key => key + "=" + props.get(key)
    }.mkString(", ") + "}"
  }
}

object MetaProperties {
  def parse(properties: RawMetaProperties): MetaProperties = {
    val clusterId = require(ClusterIdKey, properties.clusterId)
    if (properties.version == 1) {
      val nodeId = require(NodeIdKey, properties.nodeId)
      new MetaProperties(clusterId, nodeId)
    } else if (properties.version == 0) {
      val brokerId = require(BrokerIdKey, properties.brokerId)
      new MetaProperties(clusterId, brokerId)
    } else {
      throw new RuntimeException(s"Expected version 0 or 1, but got version ${properties.version}")
    }
  }

  def require[T](key: String, value: Option[T]): T = {
    value.getOrElse(throw new RuntimeException(s"Failed to find required property $key."))
  }
}

case class ZkMetaProperties(
  clusterId: String,
  brokerId: Int
) {
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 0
    properties.clusterId = clusterId
    properties.brokerId = brokerId
    properties.props
  }

  override def toString: String = {
    s"ZkMetaProperties(brokerId=$brokerId, clusterId=$clusterId)"
  }
}

case class MetaProperties(
  clusterId: String,
  nodeId: Int,
) {
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 1
    properties.clusterId = clusterId
    properties.nodeId = nodeId
    properties.props
  }

  override def toString: String  = {
    s"MetaProperties(clusterId=$clusterId, nodeId=$nodeId)"
  }
}

object BrokerMetadataCheckpoint extends Logging {

  /**
   * mark 获取Broker元数据和离线目录。
   *
   * 从给定的日志目录中读取元数据，并识别任何离线目录。如果指定了忽略缺失的元数据，
   * 则不会因未找到元数据文件而抛出异常。
   *
   * @param logDirs       日志目录的序列，从中读取元数据。
   * @param ignoreMissing 指示是否应该忽略未找到元数据的情况。
   * @param kraftMode     指示是否处于KRaft模式。
   * @return 元数据属性和离线目录的元组。如果未找到任何元数据，则元数据属性为空。
   */
  def getBrokerMetadataAndOfflineDirs(
    logDirs: collection.Seq[String],
    ignoreMissing: Boolean,
    kraftMode: Boolean
  ): (RawMetaProperties, collection.Seq[String]) = {
    // mark 确保至少有一个日志目录以读取元数据
    require(logDirs.nonEmpty, "Must have at least one log dir to read meta.properties")

    // mark 用于存储从各日志目录读取的Broker元数据 Map{logs -> Properties}
    val brokerMetadataMap = mutable.HashMap[String, Properties]()
    // mark 离线目录集合
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    // mark 遍历每个日志目录以尝试读取元数据
    for (logDir <- logDirs) {
      // mark 构建元数据文件的路径
      val brokerCheckpointFile = new File(logDir, "meta.properties")
      // mark 创建用于读取元数据的检查点对象 BrokerMetadataCheckpoint 是对File的包装提供了线程安全的读写方法
      val brokerCheckpoint = new BrokerMetadataCheckpoint(brokerCheckpointFile)

      try {
        // mark 尝试读取元数据，并根据情况将其添加到映射中或在忽略缺失时跳过
        brokerCheckpoint.read() match {
          case Some(properties) =>
            // mark 保存映射关系
            brokerMetadataMap += logDir -> properties
          case None =>
            if (!ignoreMissing) {
              throw new KafkaException(s"No `meta.properties` found in $logDir " +
                "(have you run `kafka-storage.sh` to format the directory?)")
            }
        }
      } catch {
        // mark 捕获IO异常，并将相应目录标记为离线
        case e: IOException =>
          offlineDirs += logDir
          error(s"Failed to read $brokerCheckpointFile", e)
      }
    }

    // mark 如果<Dir，Meta Properties> 映射为空（可能所有目录都已经离线：IO异常）
    if (brokerMetadataMap.isEmpty) {
      (new RawMetaProperties(), offlineDirs)
    }
    else {
      // 在KRaft模式下，检查不同目录中的元数据版本是否一致
      // KRaft mode has to support handling both meta.properties versions 0 and 1 and has to
      // reconcile have multiple versions in different directories.
      // mark numDistinctMetaProperties保存元数据版本不同目录的数量
      // mark 是将brokerMetadataMap.values进行去重 kafka不同目录下的meta.properties需要保持一致否则下面会抛出异常
      val numDistinctMetaProperties = if (kraftMode) {
        brokerMetadataMap.values.map(props => MetaProperties.parse(new RawMetaProperties(props))).toSet.size
      } else {
        brokerMetadataMap.values.toSet.size
      }
      // mark 如果 numDistinctMetaProperties > 1 说明不同目录的元数据不一样 抛出异常
      if (numDistinctMetaProperties > 1) {
        val builder = new StringBuilder

        for ((logDir, brokerMetadata) <- brokerMetadataMap)
          builder ++= s"- $logDir -> $brokerMetadata\n"

        throw new InconsistentBrokerMetadataException(
          s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
            s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
        )
      }

      // mark map中的head返回第一个键值对的元祖，元祖中的第一个 第二个值可以使用 _1,_2直接获取
      val rawProps = new RawMetaProperties(brokerMetadataMap.head._2)
      (rawProps, offlineDirs)
    }
  }

}

/**
 * mark 日志元数据文件检查点操作类（/data-log-dir/meta.properties）
 * This class saves the metadata properties to a file
 */
class BrokerMetadataCheckpoint(val file: File) extends Logging {
  private val lock = new Object()

  /**
   * 将给定的属性对象写入到指定的文件中。
   * 此方法使用同步锁来确保并发访问时的线程安全。
   * 它首先将内容写入一个临时文件，然后使用原子操作将临时文件移动到目标文件位置，
   * 这样可以减少在写入过程中文件内容部分更新的风险。
   *
   * @param properties 要写入文件的属性对象。
   */
  def write(properties: Properties): Unit = {
    // 使用同步锁来确保线程安全
    lock synchronized {
      try {
        // mark 创建一个临时文件，用于实际的写入操作
        val temp = new File(file.getAbsolutePath + ".tmp")
        // 打开文件输出流，用于向临时文件写入属性内容
        val fileOutputStream = new FileOutputStream(temp)
        try {
          // mark 将属性对象写入文件输出流
          properties.store(fileOutputStream, "")
          // mark 确保所有写入操作都刷新到磁盘
          fileOutputStream.flush()
          // 强制将文件描述符对应的文件内容同步到磁盘
          fileOutputStream.getFD.sync()
        } finally {
          // 关闭文件输出流，并删除临时文件，异常情况下也会执行
          Utils.closeQuietly(fileOutputStream, temp.getName)
        }
        // mark 使用原子操作将临时文件移动到目标文件位置，以替代直接写入，提高文件完整性
        Utils.atomicMoveWithFallback(temp.toPath, file.toPath)
      } catch {
        case ie: IOException =>
          // 记录写入失败的错误信息，并重新抛出异常
          error("Failed to write meta.properties due to", ie)
          throw ie
      }
    }
  }

  /**
   * 根据文件路径读取并返回属性文件。
   * 若文件不存在或在读取过程中发生错误，将返回None或抛出异常。
   * 在读取前尝试删除任何存在的临时文件，以保持文件系统的整洁。
   *
   * @return 文件存在且读取成功时返回Some(Properties)，否则返回None。
   * @throws Exception 当文件读取过程中发生错误时抛出异常。
   */
  def read(): Option[Properties] = {
    // mark 如果存在临时文件中则删除
    Files.deleteIfExists(new File(file.getPath + ".tmp").toPath) // 尝试删除任何存在的临时文件以保持整洁

    // mark 获取文件的绝对路径
    val absolutePath = file.getAbsolutePath
    // mark 使用锁来确保在读取文件时的线程安全性
    lock synchronized {
      try {
        // mark loadProps 可以读取properties文件
        Some(Utils.loadProps(absolutePath))
      } catch {
        case _: NoSuchFileException =>
          // 如果文件不存在，发出警告并返回None
          warn(s"在目录 $absolutePath 下未找到meta.properties文件")
          None
        case e: Exception =>
          // 其他异常发生时，记录错误并抛出异常
          error(s"未能读取目录 $absolutePath 下的meta.properties文件", e)
          throw e
      }
    }
  }

}
