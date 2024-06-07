/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import org.apache.kafka.common.{KafkaException, Endpoint => JEndpoint}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import java.util.Locale
import scala.collection.Map

object EndPoint {

  private val uriParseExp = """^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)""".r

  private[kafka] val DefaultSecurityProtocolMap: Map[ListenerName, SecurityProtocol] =
    SecurityProtocol.values.map(sp => ListenerName.forSecurityProtocol(sp) -> sp).toMap

  /**
   * 根据连接字符串和安全协议映射创建EndPoint对象。
   *
   * 连接字符串的格式为listener_name://host:port，其中listener_name是监听器名称，host是主机地址，port是端口号。
   * 如果未提供安全协议映射，则使用默认的安全协议映射。
   *
   * @param connectionString    连接字符串，描述了监听器名称、主机地址和端口号。
   * @param securityProtocolMap 安全协议映射，可选，映射监听器名称到安全协议。
   * @return 返回一个EndPoint对象，表示连接的端点。
   * @throws IllegalArgumentException 如果未为指定的监听器定义安全协议，则抛出此异常。
   * @throws KafkaException           如果无法解析连接字符串为有效的Broker端点，则抛出此异常。
   */
  def createEndPoint(connectionString: String, securityProtocolMap: Option[Map[ListenerName, SecurityProtocol]]): EndPoint = {
    // mark 使用提供的安全协议映射，或默认映射。
    val protocolMap = securityProtocolMap.getOrElse(DefaultSecurityProtocolMap)

    // mark 根据监听器名称获取安全协议，如果未定义则抛出异常。
    def securityProtocol(listenerName: ListenerName): SecurityProtocol =
      protocolMap.getOrElse(listenerName,
        throw new IllegalArgumentException(s"No security protocol defined for listener ${listenerName.value}"))

    // 解析连接字符串并创建EndPoint对象。
    connectionString match {
      // mark 解析不包含主机地址的连接字符串（只有监听器名称和端口）。
      // mark scala的正则匹配非常简单 定义正则表达式 pattern = ^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)
      // mark str match {case pattern(a, b, c)} 采用这种模式 scala会自动帮你解析出匹配的组并且绑定到a,b,c中
      case uriParseExp(listenerNameString, "", port) =>
        val listenerName = ListenerName.normalised(listenerNameString)
        new EndPoint(null, port.toInt, listenerName, securityProtocol(listenerName))
      // mark 解析包含主机地址的连接字符串。
      case uriParseExp(listenerNameString, host, port) =>
        // mark 根据监听器名称创建 ListenerName 对象
        val listenerName = ListenerName.normalised(listenerNameString)
        // mark 根据 host port 监听器名称 协议名称创建EndPoint对象
        new EndPoint(host, port.toInt, listenerName, securityProtocol(listenerName))
      // mark 如果连接字符串格式无效，则抛出异常。
      case _ => throw new KafkaException(s"Unable to parse $connectionString to a broker endpoint")
    }
  }

  def parseListenerName(connectionString: String): String = {
    connectionString match {
      case uriParseExp(listenerNameString, _, _) => listenerNameString.toUpperCase(Locale.ROOT)
      case _ => throw new KafkaException(s"Unable to parse a listener name from $connectionString")
    }
  }

  def fromJava(endpoint: JEndpoint): EndPoint =
    new EndPoint(endpoint.host(),
      endpoint.port(),
      new ListenerName(endpoint.listenerName().get()),
      endpoint.securityProtocol())
}

/**
 * Part of the broker definition - matching host/port pair to a protocol
 */
case class EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol) {

  /**
   * 构建连接字符串。
   *
   * 此方法用于根据将EndPoint转化为字符串
   * 连接字符串的格式为“listenerName://host:port”，其中listenerName是监听器的名称，
   * host是主机地址，port是端口号。如果host为null，则只显示端口号。
   *
   * @return 返回构建好的连接字符串。
   */
  def connectionString: String = {
    // 根据host是否为null，决定如何构造hostport部分
    val hostport =
      if (host == null)
        ":" + port
      else
        Utils.formatAddress(host, port)
    // 组合监听器名称和hostport，形成最终的连接字符串
    listenerName.value + "://" + hostport
  }

  def toJava: JEndpoint = {
    new JEndpoint(listenerName.value, securityProtocol, host, port)
  }
}
