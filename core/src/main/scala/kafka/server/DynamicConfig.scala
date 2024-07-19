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

package kafka.server

import java.net.{InetAddress, UnknownHostException}
import java.util.Properties

import kafka.log.LogConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance._
import org.apache.kafka.common.config.ConfigDef.Range._
import org.apache.kafka.common.config.ConfigDef.Type._

import scala.jdk.CollectionConverters._

/**
 * mark 这里类里面保存的是仅仅只能动态配置的属性
  * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
  * and can only be set dynamically.
  */
object DynamicConfig {

  object Broker {
    // Properties
    // mark 限制leader副本的同步速率
    val LeaderReplicationThrottledRateProp = "leader.replication.throttled.rate"
    // mark 限制follower副本的接收速率
    val FollowerReplicationThrottledRateProp = "follower.replication.throttled.rate"
    // mark 限制目录变更时同步数据的速率
    val ReplicaAlterLogDirsIoMaxBytesPerSecondProp = "replica.alter.log.dirs.io.max.bytes.per.second"

    // Defaults
    val DefaultReplicationThrottledRate = ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault

    // Documentation
    val LeaderReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
      s"property ${LogConfig.LeaderReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
      s"limit be kept above 1MB/s for accurate behaviour."
    val FollowerReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
      s"property ${LogConfig.FollowerReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
      s"limit be kept above 1MB/s for accurate behaviour."
    val ReplicaAlterLogDirsIoMaxBytesPerSecondDoc = "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between log directories on the same broker. " +
      s"This property can be only set dynamically. It is suggested that the limit be kept above 1MB/s for accurate behaviour."

    // Definitions
    val brokerConfigDef = new ConfigDef()
      // Round minimum value down, to make it easier for users.
      .define(LeaderReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, LeaderReplicationThrottledRateDoc)
      .define(FollowerReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, FollowerReplicationThrottledRateDoc)
      .define(ReplicaAlterLogDirsIoMaxBytesPerSecondProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, ReplicaAlterLogDirsIoMaxBytesPerSecondDoc)
    // mark 这里会把独有的三个配置和可以动态配置相加（所有的动态配置）
    DynamicBrokerConfig.addDynamicConfigs(brokerConfigDef)
    // mark KafkaConfig.configNames为所有的kafka配置 减去动态配置 则获得非动态配置集合
    val nonDynamicProps = KafkaConfig.configNames.toSet -- brokerConfigDef.names.asScala

    def names = brokerConfigDef.names

    def validate(props: Properties) = DynamicConfig.validate(brokerConfigDef, props, customPropsAllowed = true)
  }

  object QuotaConfigs {
    def isClientOrUserQuotaConfig(name: String): Boolean = org.apache.kafka.common.config.internals.QuotaConfigs.isClientOrUserConfig(name)
  }

  object Client {
    private val clientConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.clientConfigs()

    def configKeys = clientConfigs.configKeys

    def names = clientConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(clientConfigs, props, customPropsAllowed = false)
  }

  object User {
    private val userConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.userConfigs()

    def configKeys = userConfigs.configKeys

    def names = userConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(userConfigs, props, customPropsAllowed = false)
  }

  object Ip {
    private val ipConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.ipConfigs()

    def configKeys = ipConfigs.configKeys

    def names = ipConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(ipConfigs, props, customPropsAllowed = false)

    def isValidIpEntity(ip: String): Boolean = {
      if (ip != ConfigEntityName.Default) {
        try {
          InetAddress.getByName(ip)
        } catch {
          case _: UnknownHostException => return false
        }
      }
      true
    }
  }

  /**
   * mark 验证配置属性是否符合指定的配置定义。
   *
   * 这个方法检查给定的配置属性是否符合 `ConfigDef` 定义，并根据是否允许自定义属性来处理未知的配置键。
   * 该方法还解析变量配置并验证最终的配置值。
   *
   * @param configDef          定义有效配置键及其约束的 `ConfigDef` 对象。
   * @param props              包含待验证配置的 `Properties` 对象。
   * @param customPropsAllowed 一个布尔值，指示是否允许自定义属性。如果为 `false`，则不允许配置中包含未定义的键。
   * @throws IllegalArgumentException 如果不允许自定义属性且发现未知的配置键时抛出此异常。
   */
  private def validate(configDef: ConfigDef, props: Properties, customPropsAllowed: Boolean) = {
    // mark 获取有效配置名称
    val names = configDef.names()

    // mark 获取传入的配置属性中的键名集合，并将它们转换为 Scala 集合
    val propKeys = props.keySet.asScala.map(_.asInstanceOf[String])

    // mark 如果不允许自定义属性，检查是否有未知的配置键
    if (!customPropsAllowed) {
      // mark 过滤出所有未定义的键
      val unknownKeys = propKeys.filterNot(names.contains(_))

      // mark 如果存在未知的键，抛出异常
      require(unknownKeys.isEmpty, s"Unknown Dynamic Configuration: $unknownKeys.")
    }

    // 解析传入配置中的变量配置
    val propResolved = DynamicBrokerConfig.resolveVariableConfigs(props)

    // 使用配置定义解析并验证最终的配置值
    configDef.parse(propResolved)
  }
}
