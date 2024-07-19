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

package kafka.zookeeper

import java.util.Locale
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent._
import java.util.{List => JList}

import com.yammer.metrics.core.MetricName
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.{KafkaScheduler, Logging}
import kafka.zookeeper.ZooKeeperClient._
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.AsyncCallback.{Children2Callback, DataCallback, StatCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper._
import org.apache.zookeeper.client.ZKClientConfig

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.collection.mutable.Set

object ZooKeeperClient {
  val RetryBackoffMs = 1000
}

/**
 * A ZooKeeper client that encourages pipelined requests.
 *
 * @param connectString comma separated host:port pairs, each corresponding to a zk server
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param maxInFlightRequests maximum number of unacknowledged requests the client will send before blocking.
 * @param name name of the client instance
 * @param zkClientConfig ZooKeeper client configuration, for TLS configs if desired
 */
class ZooKeeperClient(connectString: String,
                      sessionTimeoutMs: Int,
                      connectionTimeoutMs: Int,
                      maxInFlightRequests: Int, // zookeeper最大请求数量
                      time: Time,
                      metricGroup: String,
                      metricType: String,
                      private[zookeeper] val clientConfig: ZKClientConfig,
                      name: String) extends Logging with KafkaMetricsGroup {
  // mark 日志前缀
  this.logIdent = s"[ZooKeeperClient $name] "
  // mark 准备的读写锁
  private val initializationLock = new ReentrantReadWriteLock()
  // mark 当zookeeper连接过期时使用的锁
  private val isConnectedOrExpiredLock = new ReentrantLock()

  // mark zookeeper连接过期时使用的通知条件
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala

  // mark Semaphore 用于控制请求最大数据量
  private val inFlightRequests = new Semaphore(maxInFlightRequests)
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala
  private[zookeeper] val reinitializeScheduler = new KafkaScheduler(threads = 1, s"zk-client-${threadPrefix}reinit-")
  private var isFirstConnectionEstablished = false

  private val metricNames = Set[String]()

  // 在创建ZooKeeper之前必须先创建状态映射，因为ZooKeeper回调中需要使用到它。
  /**
   * mark 生成zookeeper状态仪表映射。（卡夫卡监听zookeeper连接状态时会进行回调）
   */
  private val stateToMeterMap = {
    import KeeperState._
    // 定义从ZooKeeper连接状态到事件描述的映射。
    val stateToEventTypeMap = Map(
      Disconnected -> "Disconnects",
      SyncConnected -> "SyncConnects",
      AuthFailed -> "AuthFailures",
      ConnectedReadOnly -> "ReadOnlyConnects",
      SaslAuthenticated -> "SaslAuthentications",
      Expired -> "Expires"
    )
    // mark 遍历映射，为每种连接状态创建一个计数器。
    // mark 计数器用于测量每种事件类型发生的速率。
    stateToEventTypeMap.map { case (state, eventType) =>
      val name = s"ZooKeeper${eventType}PerSec"
      // 将计数器名称添加到metricNames集合中，以便后续引用。
      metricNames += name
      // mark 为当前连接状态和事件类型创建一个新的计数器。
      state -> newMeter(name, eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
    }
  }


  info(s"Initializing a new session to $connectString.")
  // Fail-fast if there's an error during construction (so don't call initialize, which retries forever)
  // mark 初始化zookeeper客户端session
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher,
    clientConfig)

  newGauge("SessionState", () => connectionState.toString)

  metricNames += "SessionState"
  // mark 启动调度器
  reinitializeScheduler.startup()
  // mark 等待zookeeper连接建立
  try waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)
  catch {
    case e: Throwable =>
      close()
      throw e
  }

  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName(metricGroup, metricType, name, metricTags)
  }

  /**
   * mark 获取zookeeper的连接状态
   * Return the state of the ZooKeeper connection.
   */
  def connectionState: States = zooKeeper.getState

  /**
   * mark 处理单个请求
   * Send a request and wait for its response. See handle(Seq[AsyncRequest]) for details.
   *
   * @param request a single request to send and wait on.
   * @return an instance of the response with the specific type (e.g. CreateRequest -> CreateResponse).
   */
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    handleRequests(Seq(request)).head
  }

  /**
   * mark 处理批量请求.
   *
   * Send a pipelined sequence of requests and wait for all of their responses.
   *
   * The watch flag on each outgoing request will be set if we've already registered a handler for the
   * path associated with the request.
   *
   * @param requests a sequence of requests to send and wait on.
   * @return the responses for the requests. If all requests have the same type, the responses will have the respective
   * response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
   * will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
   */
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    if (requests.isEmpty)
      Seq.empty
    else {
      // mark 准备栅栏和响应队列
      val countDownLatch = new CountDownLatch(requests.size)
      val responseQueue = new ArrayBlockingQueue[Req#Response](requests.size)

      requests.foreach { request =>
        // mark 最大请求数加1
        inFlightRequests.acquire()
        try {
          // mark 异步发送请求
          inReadLock(initializationLock) {
            send(request) { response =>
              // mark send方法中异步请求zookeeper完成后处理response的回调
              responseQueue.add(response) // mark 加入队列
              inFlightRequests.release() // mark 释放请求数
              countDownLatch.countDown() // mark 栅栏减1
            }
          }
        } catch {
          case e: Throwable =>
            inFlightRequests.release()
            throw e
        }
      }
      countDownLatch.await()
      responseQueue.asScala.toBuffer
    }
  }

  /**
   * 发送一个异步请求到ZooKeeper，并处理响应。
   *
   * 这个方法接受一个类型为 `AsyncRequest` 的请求和一个回调函数 `processResponse`，用于处理响应。
   * 它会匹配请求的类型，发送相应的ZooKeeper请求，并附加适当的回调来处理响应。
   * 然后将响应传递给提供的 `processResponse` 函数进行进一步处理。
   *
   * @param request         异步请求，必须是 `AsyncRequest` 的子类型。
   * @param processResponse 处理响应的函数，类型为 `Req#Response`。
   *                        当从ZooKeeper接收到响应时，这个函数会被调用来处理响应。
   */
  private[zookeeper] def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
    // Safe to cast as we always create a response of the right type
    // mark zookeeper异步发送用于处理response的回调
    def callback(response: AsyncResponse): Unit = processResponse(response.asInstanceOf[Req#Response])

    def responseMetadata(sendTimeMs: Long) = new ResponseMetadata(sendTimeMs, receivedTimeMs = time.hiResClockMs())

    val sendTimeMs = time.hiResClockMs()

    // Cast to AsyncRequest to workaround a scalac bug that results in an false exhaustiveness warning
    // with -Xlint:strict-unsealed-patmat
    (request: AsyncRequest) match {
      case ExistsRequest(path, ctx) =>
        zooKeeper.exists(path, shouldWatch(request), new StatCallback {
          def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(ExistsResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetDataRequest(path, ctx) =>
        zooKeeper.getData(path, shouldWatch(request), new DataCallback {
          def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat): Unit =
            callback(GetDataResponse(Code.get(rc), path, Option(ctx), data, stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetChildrenRequest(path, _, ctx) =>
        zooKeeper.getChildren(path, shouldWatch(request), new Children2Callback {
          def processResult(rc: Int, path: String, ctx: Any, children: JList[String], stat: Stat): Unit =
            callback(GetChildrenResponse(Code.get(rc), path, Option(ctx), Option(children).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case CreateRequest(path, data, acl, createMode, ctx) =>
        zooKeeper.create(path, data, acl.asJava, createMode,
          (rc, path, ctx, name) =>
            callback(CreateResponse(Code.get(rc), path, Option(ctx), name, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case SetDataRequest(path, data, version, ctx) =>
        zooKeeper.setData(path, data, version,
          (rc, path, ctx, stat) =>
            callback(SetDataResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case DeleteRequest(path, version, ctx) =>
        zooKeeper.delete(path, version,
          (rc, path, ctx) => callback(DeleteResponse(Code.get(rc), path, Option(ctx), responseMetadata(sendTimeMs))),
          ctx.orNull)
      case GetAclRequest(path, ctx) =>
        zooKeeper.getACL(path, null,
          (rc, path, ctx, acl, stat) =>
            callback(GetAclResponse(Code.get(rc), path, Option(ctx), Option(acl).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case SetAclRequest(path, acl, version, ctx) =>
        zooKeeper.setACL(path, acl.asJava, version,
          (rc, path, ctx, stat) =>
            callback(SetAclResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case MultiRequest(zkOps, ctx) =>
        def toZkOpResult(opResults: JList[OpResult]): Seq[ZkOpResult] =
          Option(opResults).map(results => zkOps.zip(results.asScala).map { case (zkOp, result) =>
            ZkOpResult(zkOp, result)
          }).orNull
        zooKeeper.multi(zkOps.map(_.toZookeeperOp).asJava,
          (rc, path, ctx, opResults) =>
            callback(MultiResponse(Code.get(rc), path, Option(ctx), toZkOpResult(opResults), responseMetadata(sendTimeMs))),
          ctx.orNull)
    }
  }

  /**
   * mark 无期限的等待zookeeper连接成功
   *
   * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
   *
   * @throws ZooKeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
   * @throws ZooKeeperClientExpiredException if the session expired either before or while waiting for connection.
   */
  def waitUntilConnected(): Unit = inLock(isConnectedOrExpiredLock) {
    waitUntilConnected(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * 等待与ZooKeeper的连接建立，直到超时或成功连接。
   *
   * 这个方法会阻塞当前线程，直到与ZooKeeper的连接建立，或者发生连接超时或认证失败。
   *
   * @param timeout  等待连接的超时时间。
   * @param timeUnit 超时时间的单位。
   * @throws ZooKeeperClientTimeoutException    当等待连接的时间超过指定的超时时间时抛出。
   * @throws ZooKeeperClientAuthFailedException 当认证失败时抛出。
   * @throws ZooKeeperClientExpiredException    当会话过期时抛出。
   */
  private def waitUntilConnected(timeout: Long, timeUnit: TimeUnit): Unit = {
    info("Waiting until connected.")
    var nanos = timeUnit.toNanos(timeout) // 期望等待时间
    inLock(isConnectedOrExpiredLock) {
      // mark connectionState实际上是会调用zookeeper.getState()方法获取zookeeper的连接
      var state = connectionState
      while (!state.isConnected && state.isAlive) {
        // mark 时间到抛出连接超时异常
        if (nanos <= 0) {
          throw new ZooKeeperClientTimeoutException(s"Timed out waiting for connection while in state: $state")
        }
        nanos = isConnectedOrExpiredCondition.awaitNanos(nanos)
        state = connectionState
      }
      // mark 如果 state.isConnected == true 则说明连接成功
      if (state == States.AUTH_FAILED) {
        throw new ZooKeeperClientAuthFailedException("Auth failed either before or while waiting for connection")
      } else if (state == States.CLOSED) {
        throw new ZooKeeperClientExpiredException("Session expired either before or while waiting for connection")
      }
      // mark 首次已经建立连接标志位
      isFirstConnectionEstablished = true
    }
    info("Connected.")
  }

  // If this method is changed, the documentation for registerZNodeChangeHandler and/or registerZNodeChildChangeHandler
  // may need to be updated.
  private def shouldWatch(request: AsyncRequest): Boolean = request match {
    case GetChildrenRequest(_, registerWatch, _) => registerWatch && zNodeChildChangeHandlers.contains(request.path)
    case _: ExistsRequest | _: GetDataRequest => zNodeChangeHandlers.contains(request.path)
    case _ => throw new IllegalArgumentException(s"Request $request is not watchable")
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
   * with either a GetDataRequest or ExistsRequest.
   *
   * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put(zNodeChangeHandler.path, zNodeChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
   *
   * @param zNodeChildChangeHandler the handler to register
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path, zNodeChildChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove(path)
  }

  /**
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = inReadLock(initializationLock) {
    if (stateChangeHandler != null)
      stateChangeHandlers.put(stateChangeHandler.name, stateChangeHandler)
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = inReadLock(initializationLock) {
    stateChangeHandlers.remove(name)
  }

  def close(): Unit = {
    info("Closing.")

    // Shutdown scheduler outside of lock to avoid deadlock if scheduler
    // is waiting for lock to process session expiry. Close expiry thread
    // first to ensure that new clients are not created during close().
    reinitializeScheduler.shutdown()

    inWriteLock(initializationLock) {
      zNodeChangeHandlers.clear()
      zNodeChildChangeHandlers.clear()
      stateChangeHandlers.clear()
      zooKeeper.close()
      metricNames.foreach(removeMetric(_))
    }
    info("Closed.")
  }

  def sessionId: Long = inReadLock(initializationLock) {
    zooKeeper.getSessionId
  }

  // Only for testing
  private[kafka] def currentZooKeeper: ZooKeeper = inReadLock(initializationLock) {
    zooKeeper
  }

  /**
   * mark 重新初始化ZooKeeper客户端。
   * 当当前会话不再有效或已过期时，此方法用于创建一个新的ZooKeeper会话。
   * 它确保ZooKeeper客户端处于可进行后续操作的就绪状态。
   */
  private def reinitialize(): Unit = {
    // 在锁定之外调用所有状态更改处理器的初始化前回调，
    // 避免死锁风险，因为这些回调的完成可能需要额外的Zookeeper请求，
    // 这些请求将会阻塞以获取初始化锁。
    stateChangeHandlers.values.foreach(callBeforeInitializingSession _)

    inWriteLock(initializationLock) {
      if (!connectionState.isAlive) {
        zooKeeper.close()
        info(s"Initializing a new session to $connectString.")
        // 持续重试直到ZooKeeper实例化成功
        var connected = false
        while (!connected) {
          try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher, clientConfig)
            connected = true
          } catch {
            case e: Exception =>
              info("在重新创建ZooKeeper时遇到错误，短暂休眠后重试", e)
              Thread.sleep(RetryBackoffMs)
          }
        }
      }
    }

    // 调用所有状态更改处理器的初始化后回调
    stateChangeHandlers.values.foreach(callAfterInitializingSession _)
  }


  /**
   * Close the zookeeper client to force session reinitialization. This is visible for testing only.
   */
  private[zookeeper] def forceReinitialize(): Unit = {
    zooKeeper.close()
    reinitialize()
  }

  /**
   * mark 在初始化会话之前调用处理程序的方法。
   * 此方法旨在在会话初始化之前，安全地调用处理程序的beforeInitializingSession方法。
   * 如果在调用过程中发生异常，将会记录错误信息。
   *
   * @param handler StateChangeHandler 实例，它实现了beforeInitializingSession方法。
   */
  private def callBeforeInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      // mark 尝试调用处理程序的beforeInitializingSession方法。
      handler.beforeInitializingSession()
    } catch {
      case t: Throwable =>
        // 捕获在调用处理程序方法时抛出的任何异常，并记录错误信息。
        error(s"Uncaught error in handler ${handler.name}", t)
    }
  }

  private def callAfterInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.afterInitializingSession()
    } catch {
      case t: Throwable =>
        error(s"Uncaught error in handler ${handler.name}", t)
    }
  }

  // Visibility for testing

  /**
   * mark 通过调度器进行延时初始化
   *
   * 此方法用于在指定延迟后安排一个重新初始化任务。任务会被添加到重初始化调度器中，
   * 并在延迟时间后执行。任务执行时，会打印一条信息并调用重新初始化函数。
   *
   * @param name    任务名称，用于标识和管理调度器中的任务。
   * @param message 在任务执行时打印的信息，用于日志记录和问题追踪。
   * @param delayMs 任务延迟执行的时间，以毫秒为单位。
   */
  private[zookeeper] def scheduleReinitialize(name: String, message: String, delayMs: Long): Unit = {
    // mark 使用重初始化调度器安排任务，在指定延迟后执行。
    reinitializeScheduler.schedule(name, () => {
      // mark 打印信息日志。
      info(message)
      // mark 调用重新初始化函数。
      reinitialize()
    }, delayMs, period = -1L, unit = TimeUnit.MILLISECONDS)
  }

  private def threadPrefix: String = name.replaceAll("\\s", "") + "-"

  /**
   * 实现zookeeper中watcher，zookeeper中的数据变更时会以Event形式进行通知
   *
   */
  private[zookeeper] object ZooKeeperClientWatcher extends Watcher {
    /**
     * 处理来自ZooKeeper的监控事件。
     *
     * @param event 来自ZooKeeper的监控事件。
     */
    override def process(event: WatchedEvent): Unit = {
      // 在调试级别记录接收到的事件。
      debug(s"接收到事件: $event")

      // mark 根据事件路径是否存在来处理事件。
      Option(event.getPath) match {
        // mark 如果没有路径，表示连接状态发生了变化。 （交由StateChangeHandler进行处理）
        case None =>
          // mark 从事件中获取连接状态。
          val state = event.getState
          // mark 根据连接状态增加相应的meter计数器。
          stateToMeterMap.get(state).foreach(_.mark())
          // mark 通知所有等待线程连接状态已改变。
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }

          // mark 如果认证失败，进行特殊处理。
          if (state == KeeperState.AuthFailed) {
            // 记录认证失败日志。
            error(s"认证失败, 初始化状态=$isFirstConnectionEstablished 连接状态=$connectionState")
            // mark 通知所有处理器认证失败。
            stateChangeHandlers.values.foreach(_.onAuthFailure())

            // mark 根据初始连接是否建立决定立即重试初始化或安排重试。
            val initialized = inLock(isConnectedOrExpiredLock) {
              isFirstConnectionEstablished
            }
            if (initialized && !connectionState.isAlive)
              scheduleReinitialize("auth-failed", "由于认证失败重新初始化.", RetryBackoffMs)
          } else if (state == KeeperState.Expired) {
            // 如果会话过期，安排无延迟的重新初始化。
            scheduleReinitialize("session-expired", "会话过期.", delayMs = 0L)
          }
        // 如果有路径，表示特定节点的数据或结构发生了变化。
        case Some(path) =>
          // 根据事件类型处理节点变更。
          (event.getType: @unchecked) match {
            // mark 节点子节点改变通知
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
            // mark 节点创建通知
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
            // mark 节点删除通知
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
            // mark 数据节点变更通知
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }

}

/**
 * mark 状态变更处理器接口。
 */
trait StateChangeHandler {

  // mark 处理器的名称。该属性用于唯一标识状态变更处理器。
  val name: String

  // mark 在初始化会话之前调用的方法。
  def beforeInitializingSession(): Unit = {}

  // mark 在初始化会话之后调用的方法。
  def afterInitializingSession(): Unit = {}

  // mark 认证失败时调用的方法
  def onAuthFailure(): Unit = {}
}

/**
 * mark ZNodeChangeHandler 类定义了 ZooKeeper 数据节点变化时的处理行为。
 */
trait ZNodeChangeHandler {
  // mark 节点的路径，用于标识特定的ZNode
  val path: String

  // mark 当节点被创建时调用的方法。
  def handleCreation(): Unit = {}

  // mark 当节点被删除时调用的方法。
  def handleDeletion(): Unit = {}

  // mark 当节点数据发生变更时调用的方法。
  def handleDataChange(): Unit = {}
}

/**
 * ZNodeChildChangeHandler 特质定义了处理ZNode子节点变化的接口。
 * 它适用于那些需要监控ZNode子节点变化，并在变化发生时执行特定操作的场景。
 */
trait ZNodeChildChangeHandler {
  val path: String

  // mark ZNode的子节点发生变化时被调用。
  def handleChildChange(): Unit = {}
}

// Thin wrapper for zookeeper.Op
sealed trait ZkOp {
  def toZookeeperOp: Op
}

case class CreateOp(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode) extends ZkOp {
  override def toZookeeperOp: Op = Op.create(path, data, acl.asJava, createMode)
}

case class DeleteOp(path: String, version: Int) extends ZkOp {
  override def toZookeeperOp: Op = Op.delete(path, version)
}

case class SetDataOp(path: String, data: Array[Byte], version: Int) extends ZkOp {
  override def toZookeeperOp: Op = Op.setData(path, data, version)
}

case class CheckOp(path: String, version: Int) extends ZkOp {
  override def toZookeeperOp: Op = Op.check(path, version)
}

case class ZkOpResult(zkOp: ZkOp, rawOpResult: OpResult)

sealed trait AsyncRequest {
  /**
   * This type member allows us to define methods that take requests and return responses with the correct types.
   * See ``ZooKeeperClient.handleRequests`` for example.
   */
  type Response <: AsyncResponse
  def path: String
  def ctx: Option[Any]
}

case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode,
                         ctx: Option[Any] = None) extends AsyncRequest {
  type Response = CreateResponse
}

case class DeleteRequest(path: String, version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = DeleteResponse
}

case class ExistsRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = ExistsResponse
}

case class GetDataRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetDataResponse
}

case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetDataResponse
}

case class GetAclRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetAclResponse
}

case class SetAclRequest(path: String, acl: Seq[ACL], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetAclResponse
}

case class GetChildrenRequest(path: String, registerWatch: Boolean, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetChildrenResponse
}

case class MultiRequest(zkOps: Seq[ZkOp], ctx: Option[Any] = None) extends AsyncRequest {
  type Response = MultiResponse

  override def path: String = null
}


sealed abstract class AsyncResponse {
  def resultCode: Code
  def path: String
  def ctx: Option[Any]

  /** Return None if the result code is OK and KeeperException otherwise. */
  def resultException: Option[KeeperException] =
    if (resultCode == Code.OK) None else Some(KeeperException.create(resultCode, path))

  /**
   * Throw KeeperException if the result code is not OK.
   */
  def maybeThrow(): Unit = {
    if (resultCode != Code.OK)
      throw KeeperException.create(resultCode, path)
  }

  def metadata: ResponseMetadata
}

case class ResponseMetadata(sendTimeMs: Long, receivedTimeMs: Long) {
  def responseTimeMs: Long = receivedTimeMs - sendTimeMs
}

case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String,
                          metadata: ResponseMetadata) extends AsyncResponse
case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any],
                          metadata: ResponseMetadata) extends AsyncResponse
case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse
case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse
case class GetAclResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class SetAclResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat,
                               metadata: ResponseMetadata) extends AsyncResponse
case class MultiResponse(resultCode: Code, path: String, ctx: Option[Any], zkOpResults: Seq[ZkOpResult],
                         metadata: ResponseMetadata) extends AsyncResponse

class ZooKeeperClientException(message: String) extends RuntimeException(message)
class ZooKeeperClientExpiredException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientAuthFailedException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientTimeoutException(message: String) extends ZooKeeperClientException(message)
