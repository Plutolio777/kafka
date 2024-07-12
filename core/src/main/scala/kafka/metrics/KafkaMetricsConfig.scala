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

import kafka.server.{Defaults, KafkaConfig}
import kafka.utils.{CoreUtils, VerifiableProperties}
import scala.collection.Seq

/**
 * KafkaMetricsConfig 类用于配置 Kafka 的指标报告和轮询间隔。
 *
 * @param props VerifiableProperties 对象，用于读取配置属性。
 */
class KafkaMetricsConfig(props: VerifiableProperties) {

  /**
   * 解析并获取配置的指标报告器类名列表。
   *
   * 这些报告器类名以逗号分隔，配置项用于实例化和注册 KafkaMetricsReporter 的实现类。
   * 如果没有配置，默认使用预设的报告器类列表。
   *
   * @return 一个字符串序列，包含所有配置的指标报告器类名。
   */
  /**
   * mark 解析自定义的指标报告器 需实现 [[org.apache.kafka.common.metrics.MetricsReporter]] 接口
   * Comma-separated list of reporter types. These classes should be on the
   * classpath and will be instantiated at run-time.
   *
   */
  val reporters: Seq[String] = CoreUtils.parseCsvList(props.getString(KafkaConfig.KafkaMetricsReporterClassesProp,
    Defaults.KafkaMetricReporterClasses))

  /**
   * 获取指标轮询间隔，以秒为单位。
   *
   * 这个配置决定了指标收集的频率。如果没有配置，默认使用预设的轮询间隔。
   *
   * @return 指标轮询间隔，以秒为单位。
   */
  /**
   * mark 指标轮询间隔 单位为s
   * The metrics polling interval (in seconds).
   */
  val pollingIntervalSecs: Int = props.getInt(KafkaConfig.KafkaMetricsPollingIntervalSecondsProp,
    Defaults.KafkaMetricsPollingIntervalSeconds)
}
