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

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer, Server}
import kafka.utils.Implicits._
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Time, Utils}

import scala.jdk.CollectionConverters._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")
    // mark 如果args为空的话，则打印帮助信息
    if (args.isEmpty || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser,
        "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
    }
    // mark 如果args为--version话，则打印帮助信息
    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }
    // mark 解析xxx.properties配置文件
    val props = Utils.loadProps(args(0))

    // mark 默认第一个参数是kafka的配置文件 如果后面还有参数则需要继续处理
    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      // mark 如果后面无选项参数个数大于0则打印帮助文档并退出 如-x -a -w
      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }
      // mark 只支持 key=value格式的参数 并增加到props中 启动命令行中的配置优先级更高！！！
      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    // scala 没有return??? 真抽象//
    props
  }

  // For Zk mode, the API forwarding is currently enabled only under migration flag. We can
  // directly do a static IBP check to see API forwarding is enabled here because IBP check is
  // static in Zk mode.
  private def enableApiForwarding(config: KafkaConfig) =
    config.migrationEnabled && config.interBrokerProtocolVersion.isApiForwardingEnabled

  /**
   * 根据提供的属性构建Kafka服务器实例。
   *
   * @param props 包含Kafka服务器配置的属性对象。
   * @return 根据配置要求返回一个KafkaServer或者KafkaRaftServer实例。
   */
  private def buildServer(props: Properties): Server = {
    // mark Properties -> KafkaConfig对象
    val config = KafkaConfig.fromProps(props, doLog = false)

    // mark 根据配置决定创建的服务器类型
    if (config.requiresZookeeper) {
      // mark 如果配置表明需要ZooKeeper，则创建传统的KafkaServer
      new KafkaServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None,
        enableForwarding = enableApiForwarding(config)
      )
    } else {
      // mark 如果不需要ZooKeeper，则创建使用Raft协议的KafkaRaftServer
      new KafkaRaftServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None
      )
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      // mark 1.从配置文件中中加载配置 2.从命令行中加载配置
      val serverProps = getPropsFromArgs(args)
      // mark 构建BrokerServer
      val server = buildServer(serverProps)

      try {
        // mark 针对操作系统和JDK发行版做一些适配
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }
      // mark 添加java进程退出的hook，这里检测到服务退出的时候会调用 server.shutdown()
      // attach shutdown handler to catch terminating signals as well as normal termination
      Exit.addShutdownHook("kafka-shutdown-hook", {
        try server.shutdown()
        catch {
          case _: Throwable =>
            fatal("Halting Kafka.")
            // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
            Exit.halt(1)
        }
      })

      // mark 服务启动
      try server.startup()
      catch {
        case e: Throwable =>
          // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
          fatal("Exiting Kafka due to fatal exception during startup.", e)
          Exit.exit(1)
      }
      // mark 等待服务关闭
      server.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
