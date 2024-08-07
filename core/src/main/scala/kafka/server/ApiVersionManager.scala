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

import kafka.network
import kafka.network.RequestChannel
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse

import scala.jdk.CollectionConverters._

/**
 * ApiVersionManager 特性用于管理API版本相关信息
 * 它主要负责确定哪些API被启用，以及提供关于API版本的响应信息
 */
trait ApiVersionManager {

  /**
   * 返回监听器的类型
   *
   * @return ListenerType 类型，表示监听器的种类 [[ListenerType]]
   */
  def listenerType: ListenerType

  /**
   * 获取所有被启用的API集合
   *
   * @return 一个集合.Set，包含了所有被启用的ApiKeys
   */
  def enabledApis: collection.Set[ApiKeys]

  /**
   * 根据节流时间生成API版本响应
   *
   * @param throttleTimeMs 节流时间，单位为毫秒
   * @return ApiVersionsResponse 对象，包含了API版本响应信息
   */
  def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse

  /**
   * 检查指定的API是否被启用
   *
   * @param apiKey 要检查的API键
   * @return Boolean 值，表示API是否被启用
   */
  def isApiEnabled(apiKey: ApiKeys): Boolean = enabledApis.contains(apiKey)

  /**
   * 创建新的请求度量对象
   *
   * @return RequestChannel.Metrics 对象，用于追踪和度量网络请求
   */
  def newRequestMetrics: RequestChannel.Metrics = new network.RequestChannel.Metrics(enabledApis)
}


object ApiVersionManager {


  /**
   * 创建ApiVersionManager的工厂方法
   *
   * 本方法负责根据传入的监听器类型、配置、转发管理器、支持的特性和元数据缓存来初始化一个ApiVersionManager实例
   * 主要用于管理和维护Kafka broker的API版本信息
   *
   * @param listenerType      监听器类型，可以是ZK_BROKER、BROKER或CONTROLLER
   * @param config            Kafka配置对象，用于配置ApiVersionManager
   * @param forwardingManager 转发管理器，用于处理请求转发，可以为空
   * @param supportedFeatures Broker支持的特性集合 特性集合支持实时更新详见 [[FinalizedFeatureChangeListener.initOrThrow()]]
   * @param metadataCache     元数据缓存，用于存储和查询元数据
   * @return 初始化后的ApiVersionManager实例
   */
  def apply(
             listenerType: ListenerType, // mark 监听类型 有三种 ZK_BROKER, BROKER, CONTROLLER;
             config: KafkaConfig, // mark kafka配置
             forwardingManager: Option[ForwardingManager], // mark 转发管理器
             supportedFeatures: BrokerFeatures, //
    metadataCache: MetadataCache
  ): ApiVersionManager = {
    new DefaultApiVersionManager(
      listenerType,
      forwardingManager,
      supportedFeatures,
      metadataCache
    )
  }
}

class SimpleApiVersionManager(
  val listenerType: ListenerType,
  val enabledApis: collection.Set[ApiKeys],
  brokerFeatures: Features[SupportedVersionRange]
) extends ApiVersionManager {

  def this(listenerType: ListenerType) = {
    this(listenerType, ApiKeys.apisForListener(listenerType).asScala, BrokerFeatures.defaultSupportedFeatures())
  }

  private val apiVersions = ApiVersionsResponse.collectApis(enabledApis.asJava)

  override def apiVersionResponse(requestThrottleMs: Int): ApiVersionsResponse = {
    ApiVersionsResponse.createApiVersionsResponse(requestThrottleMs, apiVersions, brokerFeatures)
  }
}

/**
 * 默认的API版本管理器类
 * 该类负责根据监听器类型、转发管理器、代理功能和元数据缓存来管理API版本
 *
 * @param listenerType      监听器类型，表示API通信的端点类型
 * @param forwardingManager 转发管理器的可选项，用于处理跨域请求转发
 * @param features          代理功能集合，提供了对不同功能的支持情况
 * @param metadataCache     元数据缓存，存储关于API的元数据信息
 */
class DefaultApiVersionManager(
  val listenerType: ListenerType,
  forwardingManager: Option[ForwardingManager],
  features: BrokerFeatures,
  metadataCache: MetadataCache
) extends ApiVersionManager {

  override def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse = {
    val supportedFeatures = features.supportedFeatures
    val finalizedFeatures = metadataCache.features()
    val controllerApiVersions = forwardingManager.flatMap(_.controllerApiVersions)

    ApiVersionsResponse.createApiVersionsResponse(
        throttleTimeMs,
        metadataCache.metadataVersion().highestSupportedRecordVersion,
        supportedFeatures,
        finalizedFeatures.features.map(kv => (kv._1, kv._2.asInstanceOf[java.lang.Short])).asJava,
        finalizedFeatures.epoch,
        controllerApiVersions.orNull,
        listenerType)
  }

  override def enabledApis: collection.Set[ApiKeys] = {
    ApiKeys.apisForListener(listenerType).asScala
  }

  override def isApiEnabled(apiKey: ApiKeys): Boolean = {
    apiKey.inScope(listenerType)
  }
}
