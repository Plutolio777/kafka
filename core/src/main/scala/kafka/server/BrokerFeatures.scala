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

import kafka.utils.Logging
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.server.common.MetadataVersion

import java.util
import scala.jdk.CollectionConverters._

/**
 * A class that encapsulates the latest features supported by the Broker and also provides APIs to
 * check for incompatibilities between the features supported by the Broker and finalized features.
 * This class is immutable in production. It provides few APIs to mutate state only for the purpose
 * of testing.
 */
class BrokerFeatures private (@volatile var supportedFeatures: Features[SupportedVersionRange]) {
  // For testing only.
  def setSupportedFeatures(newFeatures: Features[SupportedVersionRange]): Unit = {
    val combined = new util.HashMap[String, SupportedVersionRange](supportedFeatures.features())
    combined.putAll(newFeatures.features())
    supportedFeatures = Features.supportedFeatures(combined)
  }

  /**
   * Returns the default finalized features that a new Kafka cluster with IBP config >= IBP_2_7_IV0
   * needs to be bootstrapped with.
   */
  def defaultFinalizedFeatures: Map[String, Short] = {
    supportedFeatures.features.asScala.map {
      case(name, versionRange) => (name, versionRange.max)
    }.toMap
  }

  /**
   * Returns the set of feature names found to be incompatible.
   * A feature incompatibility is a version mismatch between the latest feature supported by the
   * Broker, and a provided finalized feature. This can happen because a provided finalized
   * feature:
   *  1) Does not exist in the Broker (i.e. it is unknown to the Broker).
   *           [OR]
   *  2) Exists but the FinalizedVersionRange does not match with the SupportedVersionRange
   *     of the supported feature.
   *
   * @param finalized   The finalized features against which incompatibilities need to be checked for.
   *
   * @return            The subset of input features which are incompatible. If the returned object
   *                    is empty, it means there were no feature incompatibilities found.
   */
  def incompatibleFeatures(finalized: Map[String, Short]): Map[String, Short] = {
    BrokerFeatures.incompatibleFeatures(supportedFeatures, finalized, logIncompatibilities = true)
  }
}

object BrokerFeatures extends Logging {

  def createDefault(): BrokerFeatures = {
    new BrokerFeatures(defaultSupportedFeatures())
  }

  def defaultSupportedFeatures(): Features[SupportedVersionRange] = {
    Features.supportedFeatures(
      java.util.Collections.singletonMap(MetadataVersion.FEATURE_NAME,
        new SupportedVersionRange(MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(), MetadataVersion.latest().featureLevel())))
  }

  /**
   * 创建一个空的BrokerFeatures实例。
   *
   * @return BrokerFeatures 一个不支持任何特性的BrokerFeatures实例。
   */
  def createEmpty(): BrokerFeatures = {
    new BrokerFeatures(Features.emptySupportedFeatures())
  }

  /**
   * Returns true if any of the provided finalized features are incompatible with the provided
   * supported features.
   *
   * @param supportedFeatures   The supported features to be compared
   * @param finalizedFeatures   The finalized features to be compared
   *
   * @return                    - True if there are any feature incompatibilities found.
   *                            - False otherwise.
   */
  def hasIncompatibleFeatures(supportedFeatures: Features[SupportedVersionRange],
                              finalizedFeatures: Map[String, Short]): Boolean = {
    incompatibleFeatures(supportedFeatures, finalizedFeatures, logIncompatibilities = false).nonEmpty
  }

  /**
   * 检查支持的features与最终确定的features之间是否存在不兼容性。
   *
   * @param supportedFeatures    支持的功能及其版本范围。
   * @param finalizedFeatures    最终确定的功能及其版本级别。
   * @param logIncompatibilities 是否记录不兼容性信息。
   * @return 存在不兼容性的功能及其版本级别的映射。
   */
  private def incompatibleFeatures(supportedFeatures: Features[SupportedVersionRange],
                                   finalizedFeatures: Map[String, Short],
                                   logIncompatibilities: Boolean): Map[String, Short] = {
    // mark 遍历最终确定的功能，生成所有兼容feature数组
    val incompatibleFeaturesInfo = finalizedFeatures.map {
      // mark 遍历最终确定的features 获取feature名称和版本
      case (feature, versionLevels) =>
        // mark 从当前的supportedFeatures中获取版本范围
        val supportedVersions = supportedFeatures.get(feature)
        // mark 如果支持的功能不存在，则记录不兼容信息
        if (supportedVersions == null) {
          (feature, versionLevels, "{feature=%s, reason='Unsupported feature'}".format(feature))
          // mark 如果版本不兼容
        } else if (supportedVersions.isIncompatibleWith(versionLevels)) {
          // 如果支持的功能版本与最终确定的版本不兼容，则记录不兼容信息
          (feature, versionLevels, "{feature=%s, reason='%s is incompatible with %s'}".format(
            feature, versionLevels, supportedVersions))
          // mark 版本兼容
        } else {
          // 如果功能兼容，则不记录不兼容信息
          (feature, versionLevels, null)
        }
      // mark 过滤掉存在错误日志的feature信息
    }.filter{ case(_, _, errorReason) => errorReason != null}.toList

    // mark 如果需要记录不兼容性信息，并且存在不兼容性信息，则记录警告
    if (logIncompatibilities && incompatibleFeaturesInfo.nonEmpty) {
      warn("Feature incompatibilities seen: " +
           incompatibleFeaturesInfo.map { case(_, _, errorReason) => errorReason }.mkString(", "))
    }
    // mark 返回存在不兼容性的功能及其版本级别的映射
    incompatibleFeaturesInfo.map { case(feature, versionLevels, _) => (feature, versionLevels) }.toMap
  }

}
