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

import kafka.server.metadata.{FeatureCacheUpdateException, ZkMetadataCache}

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import kafka.utils.{Logging, ShutdownableThread}
import kafka.zk.{FeatureZNode, FeatureZNodeStatus, KafkaZkClient, ZkVersion}
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler}
import org.apache.kafka.common.internals.FatalExitError

import scala.concurrent.TimeoutException

/**
 * 通过 ZK 客户端监听 ZK feature节点的变化。每当从 ZK 接收到变更通知时，
 * FinalizedFeatureCache 中的features缓存将异步更新为从 ZK 读取的最新功能。
 * 缓存更新通过单个通知处理线程进行序列化。
 *
 * 这会更新 ZkMetadataCache 中缓存的功能。
 *
 * @param finalizedFeatureCache 最终功能缓存
 * @param zkClient              Zookeeper 客户端
 */
class FinalizedFeatureChangeListener(private val finalizedFeatureCache: ZkMetadataCache,
                                     private val zkClient: KafkaZkClient) extends Logging {

  /**
   * Helper class used to update the FinalizedFeatureCache.
   *
   * @param featureZkNodePath   the path to the ZK feature node to be read
   * @param maybeNotifyOnce     an optional latch that can be used to notify the caller when an
   *                            updateOrThrow() operation is over
   */
  private class FeatureCacheUpdater(featureZkNodePath: String, maybeNotifyOnce: Option[CountDownLatch]) {

    def this(featureZkNodePath: String) = this(featureZkNodePath, Option.empty)

    /**
     * 使用从 featureZkNodePath 的 ZK 节点读取的最新功能更新 FinalizedFeatureCache 中的功能缓存。
     * 如果缓存更新不成功，则抛出合适的异常。
     *
     * 注意：如果在构造函数中提供了通知器（notifier），则该方法只能成功调用一次。
     * 之后的调用将抛出异常。
     *
     * mark 这里设计的比较好 使用CountDownLatch来实现任务的生命周期
     *
     * @throws IllegalStateException       如果在构造函数中提供了非空的通知器，并且在成功调用该方法后再次调用此方法。
     * @throws FeatureCacheUpdateException 如果在更新 FinalizedFeatureCache 时发生错误。
     */
    def updateLatestOrThrow(): Unit = {
      // mark 使用CountDownLatch控制任务只被执行一次
      maybeNotifyOnce.foreach(notifier => {
        if (notifier.getCount != 1) {
          throw new IllegalStateException(
            "Can not notify after updateLatestOrThrow was called more than once successfully.")
        }
      })

      debug(s"Reading feature ZK node at path: $featureZkNodePath")
      val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(featureZkNodePath)

      // There are 4 cases:
      //
      // (empty dataBytes, valid version)       => The empty dataBytes will fail FeatureZNode deserialization.
      //                                           FeatureZNode, when present in ZK, can not have empty contents.
      // (non-empty dataBytes, valid version)   => This is a valid case, and should pass FeatureZNode deserialization
      //                                           if dataBytes contains valid data.
      // (empty dataBytes, unknown version)     => This is a valid case, and this can happen if the FeatureZNode
      //                                           does not exist in ZK.
      // (non-empty dataBytes, unknown version) => This case is impossible, since, KafkaZkClient.getDataAndVersion
      //                                           API ensures that unknown version is returned only when the
      //                                           ZK node is absent. Therefore dataBytes should be empty in such
      //                                           a case.
      // mark 如果zkVersion为未知 则清空feature
      if (version == ZkVersion.UnknownVersion) {
        info(s"Feature ZK node at path: $featureZkNodePath does not exist")
        finalizedFeatureCache.clearFeatures()
      } else {
        var maybeFeatureZNode: Option[FeatureZNode] = Option.empty
        try {
          // mark 尝试将bytes解码成FeatureZNode
          maybeFeatureZNode = Some(FeatureZNode.decode(mayBeFeatureZNodeBytes.get))
        } catch {
          // mark 解析失败则清除缓存中的feature信息
          case e: IllegalArgumentException => {
            error(s"Unable to deserialize feature ZK node at path: $featureZkNodePath", e)
            finalizedFeatureCache.clearFeatures()
          }
        }
        maybeFeatureZNode.foreach(featureZNode => {
          featureZNode.status match {
            // mark 禁用feature功能
            case FeatureZNodeStatus.Disabled => {
              info(s"Feature ZK node at path: $featureZkNodePath is in disabled status.")
              // mark 如果禁用的话还是要清楚缓存中的feature信息
              finalizedFeatureCache.clearFeatures()
            }
            // mark 启用feature功能
            case FeatureZNodeStatus.Enabled => {
              // mark 更新缓存中的features信息
              finalizedFeatureCache.updateFeaturesOrThrow(featureZNode.features.toMap, version)
            }
            case _ => throw new IllegalStateException(s"Unexpected FeatureZNodeStatus found in $featureZNode")
          }
        })
      }

      // mark 如果有 CountDownLatch 则通知一次 用于通知外部该事件是否已经完成
      maybeNotifyOnce.foreach(notifier => notifier.countDown())
    }

    /**
     * 等待直到至少有一个 updateLatestOrThrow 成功完成。如果已经有一个 updateLatestOrThrow
     * 调用成功完成，则该方法立即返回。
     *
     * @param waitTimeMs 等待操作的超时时间（毫秒）
     * @throws TimeoutException 如果在 waitTimeMs 毫秒内无法完成等待
     */
    def awaitUpdateOrThrow(waitTimeMs: Long): Unit = {
      maybeNotifyOnce.foreach(notifier => {
        // mark 等待 CountDownLatch 完成，否则超时
        if (!notifier.await(waitTimeMs, TimeUnit.MILLISECONDS)) {
          throw new TimeoutException(
            s"Timed out after waiting for ${waitTimeMs}ms for FeatureCache to be updated.")
        }
      })
    }
  }

  /**
   * mark 变更通知处理器线程，可被关闭，用于处理填充到队列中的特征节点变更通知。
   *
   * @param name 线程的名称
   */
  private class ChangeNotificationProcessorThread(name: String) extends ShutdownableThread(name = name) {
    override def doWork(): Unit = {
      try {
        // mark 不断地从queue中取出任务执行 updateLatestOrThrow 方法
        queue.take.updateLatestOrThrow()
      } catch {
        case ie: InterruptedException =>
          // 当队列为空且此线程在从队列中取元素时被阻塞，
          // 并发调用FinalizedFeatureChangeListener.close()可能会中断线程并从queue.take()引发InterruptedException。
          // 在这种情况下，如果线程正在关闭，忽略异常是安全的。我们在这里再次抛出异常，
          // 因为ShutdownableThread在关闭时会忽略它。
        throw ie
        case cacheUpdateException: FeatureCacheUpdateException =>
          // mark 处理特征ZK节点变更事件失败，将导致Broker最终退出（如果出现不兼容的Feature时会抛出该异常）
          error("Failed to process feature ZK node change event. The broker will eventually exit.", cacheUpdateException)
          // mark 抛出该异常会在ShutdownableThread中捕获并且最终退出Broker
          throw new FatalExitError(1)
        case e: Exception =>
          // mark 对于与缓存变更处理无关的异常（例如ZK会话过期），不退出程序
        warn("Unexpected exception in feature ZK node change event processing; will continue processing.", e)
      }
    }
  }

  /**
   * 继承了 ZNodeChangeHandler 当 /feature路径的创建,删除,数据变更时都会发起更新事件
   *
   * @return 无返回值
   */
  //noinspection ScalaWeakerAccess
  // Feature ZK node change handler.
  object FeatureZNodeChangeHandler extends ZNodeChangeHandler {
    override val path: String = FeatureZNode.path

    override def handleCreation(): Unit = {
      info(s"Feature ZK node created at path: $path")
      queue.add(new FeatureCacheUpdater(path))
    }

    override def handleDataChange(): Unit = {
      info(s"Feature ZK node updated at path: $path")
      queue.add(new FeatureCacheUpdater(path))
    }

    override def handleDeletion(): Unit = {
      warn(s"Feature ZK node deleted at path: $path")
      // This event may happen, rarely (ex: ZK corruption or operational error).
      // In such a case, we prefer to just log a warning and treat the case as if the node is absent,
      // and populate the FinalizedFeatureCache with empty finalized features.
      queue.add(new FeatureCacheUpdater(path))
    }
  }

  object ZkStateChangeHandler extends StateChangeHandler {
    val path: String = FeatureZNode.path // mark 用于监听/feature路径的状态改变

    override val name: String = path


    /**
     * 继承了 StateChangeHandler 当 /feature路径的状态改变是会收到通知 发起更新延时操作。
     *
     * @return 无返回值
     */
    override def afterInitializingSession(): Unit = {
      queue.add(new FeatureCacheUpdater(path))
    }
  }

  // mark feature节点变更时请求更新缓存的事件队列
  private val queue = new LinkedBlockingQueue[FeatureCacheUpdater]

  // mark 处理feature信息更新缓存的线程
  private val thread = new ChangeNotificationProcessorThread("feature-zk-node-event-process-thread")

  /**
   * 该方法初始化功能 ZK 节点更改监听器。可选地，它还确保使用功能 ZK 节点的最新内容更新一次
   * FinalizedFeatureCache（如果节点存在）。这一步有助于在 initOrThrow() 方法返回给调用者之前
   * 方便地检测代理中的功能不兼容性（如果有）。如果检测到功能不兼容性，该方法将向调用者抛出
   * 异常，代理将最终退出。
   *
   * @param waitOnceForCacheUpdateMs 等待功能缓存更新一次的毫秒数。（应大于 0）
   * @throws Exception 如果无法及时完成功能不兼容性检查
   */
  def initOrThrow(waitOnceForCacheUpdateMs: Long): Unit = {
    if (waitOnceForCacheUpdateMs <= 0) {
      throw new IllegalArgumentException(
        s"Expected waitOnceForCacheUpdateMs > 0, but provided: $waitOnceForCacheUpdateMs")
    }
    // mark 启动处理线程（zk中的数据变更了会通知到这个线程进行处理）
    thread.start()

    // mark 注册zk状态变更监听器
    zkClient.registerStateChangeHandler(ZkStateChangeHandler)
    // mark 注册zk数据变更监听器
    zkClient.registerZNodeChangeHandlerAndCheckExistence(FeatureZNodeChangeHandler)

    // mark 初始话的时候先更新一下 所以这里发了一个更新事件到队列中
    val ensureCacheUpdateOnce = new FeatureCacheUpdater(
      FeatureZNodeChangeHandler.path, Some(new CountDownLatch(1)))
    queue.add(ensureCacheUpdateOnce)
    try {
      // mark 通过 CountDownLatch 来获取更新动作是否为完成
      ensureCacheUpdateOnce.awaitUpdateOrThrow(waitOnceForCacheUpdateMs)
    } catch {
      case e: Exception => {
        close()
        throw e
      }
    }
  }

  /**
   * 关闭功能 ZK 节点更改监听器，通过从 ZK 客户端注销监听器，
   * 清空队列并关闭 ChangeNotificationProcessorThread。
   */
  def close(): Unit = {
    // mark 写在zookeeper监听处理器
    zkClient.unregisterStateChangeHandler(ZkStateChangeHandler.name)
    zkClient.unregisterZNodeChangeHandler(FeatureZNodeChangeHandler.path)
    // mark 清除队列
    queue.clear()
    // mark 关闭线程
    thread.shutdown()
  }

  // For testing only.
  def isListenerInitiated: Boolean = {
    thread.isRunning && thread.isAlive
  }

  // For testing only.
  def isListenerDead: Boolean = {
    !thread.isRunning && !thread.isAlive
  }
}
