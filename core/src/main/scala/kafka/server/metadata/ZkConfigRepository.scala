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

package kafka.server.metadata

import java.util.Properties

import kafka.server.{ConfigEntityName, ConfigType}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type


object ZkConfigRepository {
  def apply(zkClient: KafkaZkClient): ZkConfigRepository =
    new ZkConfigRepository(new AdminZkClient(zkClient))
}

class ZkConfigRepository(adminZkClient: AdminZkClient) extends ConfigRepository {

  /**
   * 该方法获取Broker配置或者Topic配置
   *
   * @param configResource 配置资源对象，包含资源类型和名称
   * @return 包含配置属性的 Properties 对象
   * @throws IllegalArgumentException 如果配置资源类型不支持，则抛出异常
   */
  override def config(configResource: ConfigResource): Properties = {
    // 根据配置资源类型获取 ZooKeeper 中的配置类型
    val configTypeForZk = configResource.`type` match {
      case Type.TOPIC => ConfigType.Topic
      case Type.BROKER => ConfigType.Broker
      case tpe => throw new IllegalArgumentException(s"Unsupported config type: $tpe")
    }

    // ZK 在 "<default>" 下存储集群配置
    val effectiveName = if (configResource.`type`.equals(Type.BROKER) && configResource.name.isEmpty) {
      ConfigEntityName.Default
    } else {
      configResource.name
    }

    // mark 从zookeeper中获取配置
    adminZkClient.fetchEntityConfig(configTypeForZk, effectiveName)
  }
}
