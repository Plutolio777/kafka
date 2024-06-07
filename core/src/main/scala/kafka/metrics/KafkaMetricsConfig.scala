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

class KafkaMetricsConfig(props: VerifiableProperties) {

  /**
   * mark 解析自定义的指标报告器 需实现 [[org.apache.kafka.common.metrics.MetricsReporter]] 接口
   * Comma-separated list of reporter types. These classes should be on the
   * classpath and will be instantiated at run-time.
   *
   */
  val reporters: Seq[String] = CoreUtils.parseCsvList(props.getString(KafkaConfig.KafkaMetricsReporterClassesProp,
    Defaults.KafkaMetricReporterClasses))

  /**
   * mark 指标轮询间隔 单位为s
   * The metrics polling interval (in seconds).
   */
  val pollingIntervalSecs: Int = props.getInt(KafkaConfig.KafkaMetricsPollingIntervalSecondsProp,
    Defaults.KafkaMetricsPollingIntervalSeconds)
}
