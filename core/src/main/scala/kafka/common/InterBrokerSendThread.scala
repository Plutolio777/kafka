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
package kafka.common

import java.util.Map.Entry
import java.util.{ArrayDeque, ArrayList, Collection, Collections, HashMap, Iterator}
import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{AuthenticationException, DisconnectException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._

/**
 * 利用非阻塞网络客户端的代理间发送线程的类。
 */
//noinspection PrivateShadow
abstract class InterBrokerSendThread(
  name: String,
  @volatile var networkClient: KafkaClient,
  requestTimeoutMs: Int,
  time: Time,
  isInterruptible: Boolean = true
) extends ShutdownableThread(name, isInterruptible) {

  private val unsentRequests = new UnsentRequests

  def generateRequests(): Iterable[RequestAndCompletionHandler]

  def hasUnsentRequests: Boolean = unsentRequests.iterator().hasNext

  override def shutdown(): Unit = {
    initiateShutdown()
    networkClient.initiateClose()
    awaitShutdown()
    networkClient.close()
  }

  /**
   * 排空生成的请求。
   * 该方法的目的是将生成的请求加入到未发送请求的队列中，以便后续进行网络发送。
   * 它不返回任何值，因为其主要作用是更新内部状态。
   *
   * 注意：此方法假设`generateRequests()`方法返回的请求是有效的，并且`request.destination`、`request.request`等字段已经被正确设置。
   */
  private def drainGeneratedRequests(): Unit = {
    // mark 生成请求并针对每个请求执行操作 generateRequests由子类实现
    generateRequests().foreach { request =>
      // 将生成的请求放入未发送请求的队列中
      unsentRequests.put(
        // mark 请求地址（Node对象）
        request.destination,
        // mark 通过networkClient创建一个客户端请求
        networkClient.newClientRequest(
          request.destination.idString, // mark 目标地址字符串
          request.request, // mark 请求消息体
          request.creationTimeMs, // mark 请求创建时间
          true, // mark 标记为生成的请求
          requestTimeoutMs, // mark 请求超时时间
          request.handler // mark 请求处理程序
        ))
    }
  }


  /**
   * 执行一次轮询操作。
   *
   * 1. 处理生成的请求，调用 `drainGeneratedRequests` 方法。
   * 2. 获取当前时间，并使用 `sendRequests` 方法发送请求，计算超时值。
   * 3. 调用 `networkClient.poll` 方法进行网络轮询，传入计算出的超时值和当前时间。
   * 4. 再次获取当前时间，检查断开连接情况。
   * 5. 处理过期的请求，调用 `failExpiredRequests` 方法。
   * 6. 清理未发送的请求，调用 `unsentRequests.clean` 方法。
   *
   * 处理异常：
   * - 如果捕获到 `DisconnectException` 且 `networkClient` 不处于活动状态，表示 `NetworkClient#initiateClose` 被调用，这是预期中的异常。
   * - 如果捕获到 `FatalExitError`，则直接抛出。
   * - 捕获其他所有 `Throwable` 异常：
   *   - 记录错误日志，并重新抛出 `FatalExitError`，以便 JVM 终止。因为在这种情况下，系统可能处于未知状态，可能有请求丢失且无法继续进展。已知和预期的错误应已被适当地处理。
   */
  protected def pollOnce(maxTimeoutMs: Long): Unit = {
    try {
      // mark 将所有需要发送的请求收集到unsentRequests队列中
      drainGeneratedRequests()
      var now = time.milliseconds()
      // mark 发送的当前时间戳以及最大超时时间 发送请求
      val timeout = sendRequests(now, maxTimeoutMs)
      networkClient.poll(timeout, now)
      now = time.milliseconds()
      checkDisconnects(now)
      failExpiredRequests(now)
      unsentRequests.clean()
    } catch {
      case _: DisconnectException if !networkClient.active() =>
        // DisconnectException is expected when NetworkClient#initiateClose is called
      case e: FatalExitError => throw e
      case t: Throwable =>
        error(s"unhandled exception caught in InterBrokerSendThread", t)
        // rethrow any unhandled exceptions as FatalExitError so the JVM will be terminated
        // as we will be in an unknown state with potentially some requests dropped and not
        // being able to make progress. Known and expected Errors should have been appropriately
        // dealt with already.
        throw new FatalExitError()
    }
  }

  /**
   * 执行主要工作流程的方法。
   *
   * 调用 `pollOnce` 方法进行轮询，设置的超时时间为 `Long.MaxValue`。
   * 这意味着方法将会阻塞直到有事件可处理或者其他停止条件被满足。
   */
  override def doWork(): Unit = {
    pollOnce(Long.MaxValue)
  }

  private def sendRequests(now: Long, maxTimeoutMs: Long): Long = {
    var pollTimeout = maxTimeoutMs
    // mark 遍历unsentRequests中的所有节点
    for (node <- unsentRequests.nodes.asScala) {
      // mark 获取该节点上的请求队列迭代器
      val requestIterator = unsentRequests.requestIterator(node)
      // mark 迭代请求队列
      while (requestIterator.hasNext) {

        val request = requestIterator.next
        // mark 询问network客户端是否准备好发送
        if (networkClient.ready(node, now)) {
          // mark 发送请求
          networkClient.send(request, now)
          // mark 从队列中移除
          requestIterator.remove()
        } else
          pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(node, now))
      }
    }
    pollTimeout
  }

  private def checkDisconnects(now: Long): Unit = {
    // any disconnects affecting requests that have already been transmitted will be handled
    // by NetworkClient, so we just need to check whether connections for any of the unsent
    // requests have been disconnected; if they have, then we complete the corresponding future
    // and set the disconnect flag in the ClientResponse
    val iterator = unsentRequests.iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      val (node, requests) = (entry.getKey, entry.getValue)
      if (!requests.isEmpty && networkClient.connectionFailed(node)) {
        iterator.remove()
        for (request <- requests.asScala) {
          val authenticationException = networkClient.authenticationException(node)
          if (authenticationException != null)
            error(s"Failed to send the following request due to authentication error: $request")
          completeWithDisconnect(request, now, authenticationException)
        }
      }
    }
  }

  private def failExpiredRequests(now: Long): Unit = {
    // clear all expired unsent requests
    val timedOutRequests = unsentRequests.removeAllTimedOut(now)
    for (request <- timedOutRequests.asScala) {
      debug(s"Failed to send the following request after ${request.requestTimeoutMs} ms: $request")
      completeWithDisconnect(request, now, null)
    }
  }

  def completeWithDisconnect(request: ClientRequest,
                             now: Long,
                             authenticationException: AuthenticationException): Unit = {
    val handler = request.callback
    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
      handler, request.destination, now /* createdTimeMs */ , now /* receivedTimeMs */ , true /* disconnected */ ,
      null /* versionMismatch */ , authenticationException, null))
  }

  def wakeup(): Unit = networkClient.wakeup()
}

case class RequestAndCompletionHandler(
  creationTimeMs: Long,
  destination: Node,
  request: AbstractRequest.Builder[_ <: AbstractRequest],
  handler: RequestCompletionHandler
)

/**
 * 用于保存未发送请求集合包装类
 *
 */
//noinspection ReferenceMustBePrefixed
private class UnsentRequests {
  // mark 保存每个节点未发送的请求 key为节点 value为双端队列
  private val unsent = new HashMap[Node, ArrayDeque[ClientRequest]]

  /**
   * 将请求放入与指定节点相关联的未发送请求队列中。
   *
   * 1. 根据给定的节点从 `unsent` 中获取对应的请求队列。
   *    - 如果请求队列为空（即该节点的请求队列尚未创建），则创建一个新的 `ArrayDeque[ClientRequest]` 实例。
   *      2. 将给定的请求添加到节点对应的请求队列中。
   *
   * @param node    要将请求放入的节点。
   * @param request 要放入队列的请求。
   */
  def put(node: Node, request: ClientRequest): Unit = {
    var requests = unsent.get(node)
    if (requests == null) {
      requests = new ArrayDeque[ClientRequest]
      unsent.put(node, requests)
    }
    requests.add(request)
  }

  def removeAllTimedOut(now: Long): Collection[ClientRequest] = {
    val expiredRequests = new ArrayList[ClientRequest]
    for (requests <- unsent.values.asScala) {
      val requestIterator = requests.iterator
      var foundExpiredRequest = false
      while (requestIterator.hasNext && !foundExpiredRequest) {
        val request = requestIterator.next
        val elapsedMs = Math.max(0, now - request.createdTimeMs)
        if (elapsedMs > request.requestTimeoutMs) {
          expiredRequests.add(request)
          requestIterator.remove()
          foundExpiredRequest = true
        }
      }
    }
    expiredRequests
  }

  def clean(): Unit = {
    val iterator = unsent.values.iterator
    while (iterator.hasNext) {
      val requests = iterator.next
      if (requests.isEmpty)
        iterator.remove()
    }
  }

  def iterator(): Iterator[Entry[Node, ArrayDeque[ClientRequest]]] = {
    unsent.entrySet().iterator()
  }

  def requestIterator(node: Node): Iterator[ClientRequest] = {
    val requests = unsent.get(node)
    if (requests == null)
      Collections.emptyIterator[ClientRequest]
    else
      requests.iterator
  }

  def nodes: java.util.Set[Node] = unsent.keySet
}
