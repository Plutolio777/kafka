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

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicReference
import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.raft.RaftManager
import kafka.server.metadata.ZkMetadataCache
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.{Node, Reconfigurable}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.common.ApiMessageAndVersion

import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

/**
 * ControllerInformation 类用于封装控制器的相关信息。
 *
 * @param node             可选的节点信息，表示控制器所绑定的节点。
 * @param listenerName     表示控制器监听的名称，用于标识不同的监听器。
 * @param securityProtocol 安全协议，定义了控制器与节点间通信的安全机制。
 * @param saslMechanism    SASL机制，用于控制器与节点间的身份验证。
 * @param isZkController   标志位，表示是否为ZooKeeper控制器，用于区分不同的控制器类型。
 */
case class ControllerInformation(
  node: Option[Node],
  listenerName: ListenerName,
  securityProtocol: SecurityProtocol,
  saslMechanism: String,
  isZkController: Boolean
)


/**
 * 提供控制器节点信息的接口。
 *
 * 该接口用于获取关于控制器节点的详细信息，这些信息可以用于管理或监控控制器的状态和功能。
 * 没有参数需要传递，因为信息是关于接口实现的具体控制器节点的。
 * 返回一个ControllerInformation对象，其中包含控制器的详细信息。
 *
 * zookeeper实现 [[MetadataCacheControllerNodeProvider]]
 * kraft实现 [[RaftControllerNodeProvider]]
 *
 */
trait ControllerNodeProvider {
  /**
   * 获取控制器信息。
   *
   * @return ControllerInformation 对象，包含控制器的详细信息。
   */
  //noinspection AccessorLikeMethodIsEmptyParen
  def getControllerInfo(): ControllerInformation
}


class MetadataCacheControllerNodeProvider(val metadataCache: ZkMetadataCache, val config: KafkaConfig) extends ControllerNodeProvider {

  // mark 获取控制器专用监听器名称 control.plane.listener.name 默认使用 inter.broker.listener.name
  private val zkControllerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
  // mark 获取监听器对应的协议
  private val zkControllerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
  // mark 获取内部专用的sasl机制 sasl.mechanism.inter.broker.protocol
  private val zkControllerSaslMechanism = config.saslMechanismInterBrokerProtocol

  private val kraftControllerListenerName = if (config.controllerListenerNames.nonEmpty)
    new ListenerName(config.controllerListenerNames.head) else null
  private val kraftControllerSecurityProtocol = Option(kraftControllerListenerName)
    .map( listener => config.effectiveListenerSecurityProtocolMap.getOrElse(
      listener, SecurityProtocol.forName(kraftControllerListenerName.value())))
    .orNull
  private val kraftControllerSaslMechanism = config.saslMechanismControllerProtocol

  private val emptyZkControllerInfo =  ControllerInformation(
    None,
    zkControllerListenerName,
    zkControllerSecurityProtocol,
    zkControllerSaslMechanism,
    isZkController = true)

  /**
   * mark 获取控制器信息。
   *
   * 此方法用于根据当前的控制器类型（ZooKeeper或KRaft），从元数据缓存中提取并返回相应的控制器信息。
   * 它首先尝试从元数据缓存中获取控制器ID，然后根据控制器ID的类型（ZkCachedControllerId或KRaftCachedControllerId）
   * 来构建相应的ControllerInformation对象。
   * 如果无法获取控制器ID，则返回一个空的ZooKeeper控制器信息对象。
   *
   * @return ControllerInformation 对象，包含控制器的相关信息，如Broker节点、监听名称、安全协议和SASL机制。
   */
  override def getControllerInfo(): ControllerInformation = {
    metadataCache.getControllerId.map {
      case ZkCachedControllerId(id) => ControllerInformation(
        metadataCache.getAliveBrokerNode(id, zkControllerListenerName),
        zkControllerListenerName,
        zkControllerSecurityProtocol,
        zkControllerSaslMechanism,
        isZkController = true)
      case KRaftCachedControllerId(id) => ControllerInformation(
        metadataCache.getAliveBrokerNode(id, kraftControllerListenerName),
        kraftControllerListenerName,
        kraftControllerSecurityProtocol,
        kraftControllerSaslMechanism,
        isZkController = false)
    }.getOrElse(emptyZkControllerInfo)
  }
}

object RaftControllerNodeProvider {
  def apply(
    raftManager: RaftManager[ApiMessageAndVersion],
    config: KafkaConfig,
    controllerQuorumVoterNodes: Seq[Node]
  ): RaftControllerNodeProvider = {
    val controllerListenerName = new ListenerName(config.controllerListenerNames.head)
    val controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap.getOrElse(controllerListenerName, SecurityProtocol.forName(controllerListenerName.value()))
    val controllerSaslMechanism = config.saslMechanismControllerProtocol
    new RaftControllerNodeProvider(
      raftManager,
      controllerQuorumVoterNodes,
      controllerListenerName,
      controllerSecurityProtocol,
      controllerSaslMechanism
    )
  }
}

/**
 * Finds the controller node by checking the metadata log manager.
 * This provider is used when we are using a Raft-based metadata quorum.
 */
class RaftControllerNodeProvider(
  val raftManager: RaftManager[ApiMessageAndVersion],
  controllerQuorumVoterNodes: Seq[Node],
  val listenerName: ListenerName,
  val securityProtocol: SecurityProtocol,
  val saslMechanism: String
) extends ControllerNodeProvider with Logging {
  val idToNode = controllerQuorumVoterNodes.map(node => node.id() -> node).toMap

  override def getControllerInfo(): ControllerInformation =
    ControllerInformation(raftManager.leaderAndEpoch.leaderId.asScala.map(idToNode),
      listenerName, securityProtocol, saslMechanism, isZkController = false)
}

object BrokerToControllerChannelManager {
  /**
   * 创建一个Broker到Controller的通道管理器实例。
   *
   * @param controllerNodeProvider 提供Controller节点信息的接口，用于确定与哪个Controller建立连接。
   * @param time                   时间管理器，用于处理与时间相关的操作，如获取当前时间等。
   * @param metrics                监控指标的接口，用于收集和报告通道的性能指标。
   * @param config                 Kafka配置参数，包含各种配置项，如Socket级别参数、日志配置等。
   * @param channelName            通道名称，用于标识和区分不同的通道。
   * @param threadNamePrefix       线程名称前缀，可选，用于定制线程池中线程的名称前缀。
   * @param retryTimeoutMs         重试超时时间，以毫秒为单位，定义了在建立连接失败后的重试间隔。
   * @return 返回一个BrokerToControllerChannelManager实例，用于管理Broker与Controller之间的通信。
   */
  def apply(
    controllerNodeProvider: ControllerNodeProvider,
    time: Time,
    metrics: Metrics,
    config: KafkaConfig,
    channelName: String,
    threadNamePrefix: Option[String],
    retryTimeoutMs: Long
  ): BrokerToControllerChannelManager = {
    new BrokerToControllerChannelManagerImpl(
      controllerNodeProvider, // 提供Controller节点信息的接口，用于确定与哪个Controller建立连接。
      time,
      metrics,
      config,
      channelName,
      threadNamePrefix,
      retryTimeoutMs
    )
  }
}

/**
 * 该接口定义了Broker与Controller之间的通信通道管理器。
 * 它提供了启动和关闭通道、获取Controller的API版本、以及发送请求给Controller的能力。
 */
trait BrokerToControllerChannelManager {

  /**
   * 启动与Controller的通信通道。
   * 该方法用于初始化并建立与Controller的连接，以便后续能够进行通信。
   */
  def start(): Unit

  /**
   * 关闭与Controller的通信通道。
   * 该方法用于清理资源，关闭与Controller的连接。
   */
  def shutdown(): Unit

  /**
   * 获取Controller的API版本信息。
   *
   * @return 可能存在的Controller API版本信息，如果无法获取则为None。
   */
  def controllerApiVersions(): Option[NodeApiVersions]

  /**
   * 向Controller发送请求，并指定请求完成后的回调处理。
   *
   * @param request  待发送的请求对象的构建者，用于生成具体的请求。
   * @param callback 请求完成后的回调处理函数，用于处理请求的响应或异常情况。
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit
}

/**
 * 此类管理代理与控制器之间的连接。它运行一个单独的
 * [[BrokerToControllerRequestThread]]，该线程使用代理的元数据缓存作为自己的元数据来查找
 * 并连接到控制器。该通道是异步的，并在后台运行网络连接。
 * 为确保控制器的响应有序，最大飞行中的请求数设置为1，因此需要注意不要让未完成的请求阻塞太久。
 */
class BrokerToControllerChannelManagerImpl(
  controllerNodeProvider: ControllerNodeProvider,
  time: Time,
  metrics: Metrics,
  config: KafkaConfig,
  channelName: String,
  threadNamePrefix: Option[String],
  retryTimeoutMs: Long
) extends BrokerToControllerChannelManager with Logging {

  // mark 日志环境
  private val logContext = new LogContext(s"[BrokerToControllerChannelManager broker=${config.brokerId} name=$channelName] ")

  // mark 手动元数据更新器
  private val manualMetadataUpdater = new ManualMetadataUpdater()

  // mark API解释集合
  private val apiVersions = new ApiVersions() // 目前为空

  /** mark 生成请求线程 [[BrokerToControllerRequestThread]] */
  private val requestThread = newRequestThread

  def start(): Unit = {
    requestThread.start()
  }

  def shutdown(): Unit = {
    requestThread.shutdown()
    info(s"Broker to controller channel manager for $channelName shutdown")
  }

  /**
   * 创建一个新的请求线程，用于与控制器建立网络连接。
   *
   * 这个方法首先根据配置信息和控制器的信息构建一个网络客户端，然后使用这个客户端创建一个请求线程。
   * 请求线程负责与控制器进行通信，包括发送请求和处理响应。
   *
   * @return 新创建的请求线程。
   */
  private[server] def newRequestThread = {
    def buildNetworkClient(controllerInfo: ControllerInformation) = {
      // mark 创建对应的通道构建器
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerInfo.securityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerInfo.listenerName,
        controllerInfo.saslMechanism,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )

      // mark 如果实现了 Reconfigurable 接口添加到可重复配置列表中（主要是在动态配置管理器里面）当配置动态变更的时候会通知进行重新配置吧
      channelBuilder match {
        case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
        case _ =>
      }

      // mark 创建selector
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        channelName,
        Map("BrokerId" -> config.brokerId.toString).asJava,
        false,
        channelBuilder,
        logContext
      )

      new NetworkClient(
        // mark kafka自定义的选择器
        selector,
        // mark 手动元数据更新器
        manualMetadataUpdater,
        // mark broker id
        config.brokerId.toString,
        // mark 每个连接最大正在请求的个数
        1,
        // mark 重新连接延时
        50,
        // mark 重新连接最大超时时间
        50,
        // mark 发送缓冲区大小 这里-1表示不限制
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        // mark 接收缓冲区大小
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        // mark 默认连接超时时间
        Math.min(Int.MaxValue, Math.min(config.controllerSocketTimeoutMs, retryTimeoutMs)).toInt, // request timeout should not exceed the provided retry timeout
        // mark 连接建立时的超时时间
        config.connectionSetupTimeoutMs,
        // mark 连接建立时最大超时时间
        config.connectionSetupTimeoutMaxMs,
        // mark 时间工具类
        time,
        // mark 是否嗅探Broker版本
        true,
        // mark API注册表
        apiVersions,
        // mark 日志上下文
        logContext
      )
    }

    // mark 生成线程名称
    val threadName = threadNamePrefix match {
      case None => s"BrokerToControllerChannelManager broker=${config.brokerId} name=$channelName"
      case Some(name) => s"$name:BrokerToControllerChannelManager broker=${config.brokerId} name=$channelName"
    }

    // mark 获取控制节点信息
    val controllerInformation = controllerNodeProvider.getControllerInfo()

    // mark 创建Broker到Controller请求处理线程
    new BrokerToControllerRequestThread(
      buildNetworkClient(controllerInformation),
      controllerInformation.isZkController,
      buildNetworkClient,
      manualMetadataUpdater,
      controllerNodeProvider,
      config,
      time,
      threadName,
      retryTimeoutMs
    )
  }

  /**
   * 向控制器发送请求。
   *
   * @param request  要发送的请求。
   * @param callback 请求完成回调。
   */
  def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    requestThread.enqueue(BrokerToControllerQueueItem(
      time.milliseconds(),
      request,
      callback
    ))
  }

  def controllerApiVersions(): Option[NodeApiVersions] = {
    requestThread.activeControllerAddress().flatMap { activeController =>
      Option(apiVersions.get(activeController.idString))
    }
  }
}

abstract class ControllerRequestCompletionHandler extends RequestCompletionHandler {

  /**
   * Fire when the request transmission time passes the caller defined deadline on the channel queue.
   * It covers the total waiting time including retries which might be the result of individual request timeout.
   */
  def onTimeout(): Unit
}

/**
 * 代表从Broker到Controller的消息队列项。
 *
 * 该类用于封装发送到Controller的请求及其相关信息，包括请求的创建时间、请求对象和回调函数。
 * 主要用于在Broker和Controller之间的通信中，作为消息队列中的元素，以异步方式处理请求。
 *
 * @param createdTimeMs 请求创建的时间戳（以毫秒为单位），用于记录请求的生成时间。
 * @param request       请求对象的构建器，用于构建具体的请求。使用抽象类Builder来允许不同类型请求的构建。
 * @param callback      请求完成后的回调函数，用于处理请求完成后的逻辑，例如响应处理、错误处理等。
 */
case class BrokerToControllerQueueItem(
                                        // mark 请求创建时间
  createdTimeMs: Long,
                                        // mark 请求对象构建器
  request: AbstractRequest.Builder[_ <: AbstractRequest],
                                        // mark 请求完成后的回调
  callback: ControllerRequestCompletionHandler
)

//noinspection PrivateShadow
class BrokerToControllerRequestThread(
  initialNetworkClient: KafkaClient,
  var isNetworkClientForZkController: Boolean,
  networkClientFactory: ControllerInformation => KafkaClient,
  metadataUpdater: ManualMetadataUpdater,
  controllerNodeProvider: ControllerNodeProvider,
  config: KafkaConfig,
  time: Time,
  threadName: String,
  retryTimeoutMs: Long
) extends InterBrokerSendThread(
  threadName,
  initialNetworkClient,
  Math.min(Int.MaxValue, Math.min(config.controllerSocketTimeoutMs, retryTimeoutMs)).toInt,
  time,
  isInterruptible = false
) {

  /**
   * 根据控制器信息可能重置网络客户端。
   * 当控制器模式发生变化（从ZooKeeper切换到KafkaRaft或相反）时，
   * 此方法用于处理该场景，此时需要重置网络客户端以适应新的控制器模式。
   *
   * @param controllerInformation 包含当前控制器的信息，包括它是否为ZooKeeper控制器。
   */
  private def maybeResetNetworkClient(controllerInformation: ControllerInformation): Unit = {
    // mark 如果控制器模式改变则需要重新创建客户端
    if (isNetworkClientForZkController != controllerInformation.isZkController) {
      debug("Controller changed to " + (if (isNetworkClientForZkController) "kraft" else "zk") + " mode. " +
        s"Resetting network client with new controller information : ${controllerInformation}")
      // Close existing network client.
      val oldClient = networkClient
      oldClient.initiateClose()
      oldClient.close()

      isNetworkClientForZkController = controllerInformation.isZkController
      updateControllerAddress(controllerInformation.node.orNull)
      controllerInformation.node.foreach(n => metadataUpdater.setNodes(Seq(n).asJava))
      networkClient = networkClientFactory(controllerInformation)
    }
  }

  // mark 请求队列（链表双端阻塞队列）
  private val requestQueue = new LinkedBlockingDeque[BrokerToControllerQueueItem]()
  // mark 活跃的控制节点
  private val activeController = new AtomicReference[Node](null)

  // Used for testing
  @volatile
  private[server] var started = false

  def activeControllerAddress(): Option[Node] = {
    Option(activeController.get())
  }

  /**
   * 更新活跃控制器的地址。
   *
   * @param newActiveController 新的活跃控制器节点。这是一个节点对象，代表了控制器的当前状态和信息。
   */
  private def updateControllerAddress(newActiveController: Node): Unit = {
    activeController.set(newActiveController)
  }

  def enqueue(request: BrokerToControllerQueueItem): Unit = {
    if (!started) {
      throw new IllegalStateException("Cannot enqueue a request if the request thread is not running")
    }
    requestQueue.add(request)
    if (activeControllerAddress().isDefined) {
      wakeup()
    }
  }

  def queueSize: Int = {
    requestQueue.size
  }

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val currentTimeMs = time.milliseconds()
    val requestIter = requestQueue.iterator()
    while (requestIter.hasNext) {
      val request = requestIter.next
      if (currentTimeMs - request.createdTimeMs >= retryTimeoutMs) {
        requestIter.remove()
        request.callback.onTimeout()
      } else {
        val controllerAddress = activeControllerAddress()
        if (controllerAddress.isDefined) {
          requestIter.remove()
          return Some(RequestAndCompletionHandler(
            time.milliseconds(),
            controllerAddress.get,
            request.request,
            handleResponse(request)
          ))
        }
      }
    }
    None
  }

  private[server] def handleResponse(queueItem: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    debug(s"Request ${queueItem.request} received $response")
    if (response.authenticationException != null) {
      error(s"Request ${queueItem.request} failed due to authentication error with controller",
        response.authenticationException)
      queueItem.callback.onComplete(response)
    } else if (response.versionMismatch != null) {
      error(s"Request ${queueItem.request} failed due to unsupported version error",
        response.versionMismatch)
      queueItem.callback.onComplete(response)
    } else if (response.wasDisconnected()) {
      updateControllerAddress(null)
      requestQueue.putFirst(queueItem)
    } else if (response.responseBody().errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
      debug(s"Request ${queueItem.request} received NOT_CONTROLLER exception. Disconnecting the " +
        s"connection to the stale controller ${activeControllerAddress().map(_.idString).getOrElse("null")}")
      // just close the controller connection and wait for metadata cache update in doWork
      activeControllerAddress().foreach { controllerAddress =>
        try {
          // We don't care if disconnect has an error, just log it and get a new network client
          networkClient.disconnect(controllerAddress.idString)
        } catch {
          case t: Throwable => error("Had an error while disconnecting from NetworkClient.", t)
        }
        updateControllerAddress(null)
      }

      requestQueue.putFirst(queueItem)
    } else {
      queueItem.callback.onComplete(response)
    }
  }

  override def doWork(): Unit = {
    // mark 使用控制节点信息提供器获取控制节点信息
    // mark 从ZkMetadataCache中获取控制节点的信息
    val controllerInformation = controllerNodeProvider.getControllerInfo()
    // mark 如果集群的控制模式改变(zk->kraft)则需要重新创建网络客户端
    maybeResetNetworkClient(controllerInformation)
    // mark 查看线程是否缓存了控制节点地址
    if (activeControllerAddress().isDefined) {
      super.pollOnce(Long.MaxValue)
      // mark 如果没有缓存地址
    } else {
      // mark 则从元数据缓存中获取节点信息
      debug("Controller isn't cached, looking for local metadata changes")
      controllerInformation.node match {
        // mark 元数据缓存中存在控制节点信息
        case Some(controllerNode) =>
          info(s"Recorded new controller, from now on will use node $controllerNode")
          // mark 先更新线程缓存
          updateControllerAddress(controllerNode)
          // mark 更新元数据更新器中的节点（NetworkClient中使用元数据更新器获取节点信息）
          metadataUpdater.setNodes(Seq(controllerNode).asJava)
        case None =>
          // need to backoff to avoid tight loops
          debug("No controller provided, retrying after backoff")
          super.pollOnce(maxTimeoutMs = 100)
      }
    }
  }

  override def start(): Unit = {
    super.start()
    started = true
  }
}
