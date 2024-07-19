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
package kafka.server

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.{KafkaMetricsContext, MetricConfig, Metrics, MetricsReporter, Sensor}
import org.apache.kafka.common.utils.Time

import java.util
import java.util.concurrent.TimeUnit


trait Server {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit
}

object Server {
  val MetricsPrefix: String = "kafka.server"
  val ClusterIdLabel: String = "kafka.cluster.id"
  val BrokerIdLabel: String = "kafka.broker.id"
  val NodeIdLabel: String = "kafka.node.id"

  /**
   * 初始化Metrics对象。
   *
   * 此函数为Metrics系统提供了一个开始，它通过使用给定的配置和时间参数来创建和配置一个新的Metrics实例。
   * Metrics是用于监控和测量系统性能的关键工具，通过收集和报告各种指标，帮助理解系统的行为和健康状况。
   *
   * @param config    Kafka配置参数，用于配置Metrics。
   * @param time      时间工具，用于处理时间相关的操作。
   * @param clusterId 集群标识，用于标识Metrics所属的集群。
   * @return 返回初始化后的Metrics对象。
   */
  def initializeMetrics(
    config: KafkaConfig,
    time: Time,
    clusterId: String
  ): Metrics = {
    // mark 创建Metrics上下文
    val metricsContext = createKafkaMetricsContext(config, clusterId)
    // mark 使用配置、时间及Metrics上下文构建Metrics实例
    buildMetrics(config, time, metricsContext)
  }

  /**
   * 根据给定的配置信息构建Metrics实例。
   *
   * Metrics是Kafka中用于收集和报告各种度量数据的核心组件。这个方法负责根据配置、时间戳和度量上下文创建一个新的Metrics实例。
   *
   * @param config         Kafka配置参数，用于配置Metrics实例。
   * @param time           时间工具，用于Metrics中的时间相关操作。
   * @param metricsContext 度量上下文，用于封装度量的上下文信息，如标签。
   * @return 返回一个新的Metrics实例，用于收集和报告度量数据。
   */
  private def buildMetrics(
    config: KafkaConfig,
    time: Time,
    metricsContext: KafkaMetricsContext
  ): Metrics = {
    // mark 根据KafkaConfig构建MetricsConfig
    val metricConfig = buildMetricsConfig(config)
    // mark 使用构建的MetricsConfig、空的MetricsReporter列表、时间工具和Metrics上下文创建Metrics实例
    new Metrics(metricConfig, new util.ArrayList[MetricsReporter](), time, true, metricsContext)
  }

  /**
   * 根据Kafka配置构建MetricConfig对象。
   *
   * 此方法通过KafkaConfig参数的配置来创建一个新的MetricConfig实例，该实例用于配置Kafka内部的指标收集。
   * 它设置了指标的样本数量、记录级别和时间窗口，这些配置影响着Kafka如何采集和报告内部指标数据。
   *
   * @param kafkaConfig Kafka的配置对象，其中包含了用于配置指标的参数。
   * @return 返回一个新的MetricConfig实例，配置了从kafkaConfig中提取的指标相关参数。
   */
  def buildMetricsConfig(
    kafkaConfig: KafkaConfig
  ): MetricConfig = {
    new MetricConfig()
      // mark 指定指标采样数
      .samples(kafkaConfig.metricNumSamples)

      // mark 指定指标记录的日志级别
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))

      // mark 指定指标采样周期
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  /**
   * 根据Kafka配置和集群ID创建KafkaMetricsContext。
   *
   * @param config    Kafka配置，包含各种运行时参数。
   * @param clusterId 集群ID，用于标识Kafka集群。
   * @return 返回一个KafkaMetricsContext实例，用于度量上下文的创建。
   */
  private[server] def createKafkaMetricsContext(
    config: KafkaConfig,
    clusterId: String
  ): KafkaMetricsContext = {
    // mark 初始化一个标签映射，用于存储度量上下文的标签。
    val contextLabels = new java.util.HashMap[String, Object]
    // mark 添加集群ID标签。
    contextLabels.put(ClusterIdLabel, clusterId)

    // mark 根据配置决定使用自管理Quorum时添加NodeId标签，否则添加BrokerId标签。
    if (config.usesSelfManagedQuorum) {
      contextLabels.put(NodeIdLabel, config.nodeId.toString)
    } else {
      contextLabels.put(BrokerIdLabel, config.brokerId.toString)
    }

    // mark 添加所有以 metrics.context. 为前缀的配置项作为标签。如何 metrics.context.{context label}=value
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    // mark 使用度量前缀和标签映射创建KafkaMetricsContext实例。
    new KafkaMetricsContext(MetricsPrefix, contextLabels)
  }


  sealed trait ProcessStatus
  case object SHUTDOWN extends ProcessStatus
  case object STARTING extends ProcessStatus
  case object STARTED extends ProcessStatus
  case object SHUTTING_DOWN extends ProcessStatus
}
