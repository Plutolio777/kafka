/**
 *
 *
 *
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

package kafka.metrics

import kafka.utils.{CoreUtils, VerifiableProperties}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer


/**
 * The base trait for Kafka Metrics Reporter MBeans, defining the JMX operations exposed by the reporter.
 * KafkaMetricsReporterMBean的作用是为Kafka指标报告器提供一个标准的MBean接口。
 * 如果需要自定义报告器实现kafka.metrics.KafkaMetricsReporter，并且希望在JMX中注册这些操作，
 * 那么自定义报告器需要实现这个MBean特征。
 */
trait KafkaMetricsReporterMBean {

  /**
   * Starts the reporter.
   * 启动报告器，开始按照指定的轮询周期收集和报告指标。
   *
   * @param pollingPeriodInSeconds The polling period in seconds for metrics collection and reporting.
   */
  def startReporter(pollingPeriodInSeconds: Long): Unit

  /**
   * Stops the reporter.
   * 停止报告器，不再收集和报告指标。
   */
  def stopReporter(): Unit

  /**
   * Gets the MBean name for registration.
   * 返回报告器的MBean名称，这个名称用于在JMX中注册报告器。
   *
   * @return The MBean name string.
   */
  /**
   *
   * @return The name with which the MBean will be registered.
   */
  def getMBeanName: String
}


/**
  * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
  */
trait KafkaMetricsReporter {
  def init(props: VerifiableProperties): Unit
}

object KafkaMetricsReporter {
  val ReporterStarted: AtomicBoolean = new AtomicBoolean(false)
  private var reporters: ArrayBuffer[KafkaMetricsReporter] = _

  /**
   * 初始化并返回KafkaMetricsReporter集合。(这里会实例化自定义的一些指标报告器)
   *
   * @param verifiableProps 这个是kafka配置集合
   * @return 返回初始化的KafkaMetricsReporter序列。
   */
  def startReporters(verifiableProps: VerifiableProperties): Seq[KafkaMetricsReporter] = {
    // mark 同步块确保同时只有一个线程可以执行Reporter的启动逻辑
    ReporterStarted synchronized {
      // mark 检查报告器是否已经启动，如果没有启动，则进行初始化
      if (!ReporterStarted.get()) {
        // mark 初始化报告器列表
        reporters = ArrayBuffer[KafkaMetricsReporter]()
        // mark 会读取 verifiableProps 中的 kafka.metrics.reporters 配置来生成对应的指标报告器
        val metricsConfig = new KafkaMetricsConfig(verifiableProps)
        if (metricsConfig.reporters.nonEmpty) {
          // mark 遍历 kafka.metrics.reporters
          metricsConfig.reporters.foreach(reporterType => {
            // mark 根据提供的全限定类名生成实例
            val reporter = CoreUtils.createObject[KafkaMetricsReporter](reporterType)
            // mark 初始化报告器 MetricsReporter.init()方法
            reporter.init(verifiableProps)
            // mark 将报告器添加到列表中
            reporters += reporter
            // mark 如果报告器实现了KafkaMetricsReporterMBean接口，则注册JMX MBean
            reporter match {
              case bean: KafkaMetricsReporterMBean => CoreUtils.registerMBean(reporter, bean.getMBeanName)
              case _ =>
            }
          })
          // mark 标记报告器启动完成
          ReporterStarted.set(true)
        }
      }
    }
    // 返回报告器列表
    reporters
  }
}

