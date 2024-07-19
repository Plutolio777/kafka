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

import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.cluster.EndPoint
import kafka.log.{LogCleaner, LogConfig, LogManager, ProducerStateManagerConfig}
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.server.DynamicBrokerConfig._
import kafka.utils.{CoreUtils, Logging, PasswordEncoder}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, SslConfigs}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.{ListenerName, ListenerReconfigurable}
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.utils.{ConfigUtils, Utils}

import scala.annotation.nowarn
import scala.collection._
import scala.jdk.CollectionConverters._

/**
  * Dynamic broker configurations are stored in ZooKeeper and may be defined at two levels:
  * <ul>
  *   <li>Per-broker configs persisted at <tt>/configs/brokers/{brokerId}</tt>: These can be described/altered
  *       using AdminClient using the resource name brokerId.</li>
  *   <li>Cluster-wide defaults persisted at <tt>/configs/brokers/&lt;default&gt;</tt>: These can be described/altered
  *       using AdminClient using an empty resource name.</li>
  * </ul>
  * The order of precedence for broker configs is:
  * <ol>
  *   <li>DYNAMIC_BROKER_CONFIG: stored in ZK at /configs/brokers/{brokerId}</li>
  *   <li>DYNAMIC_DEFAULT_BROKER_CONFIG: stored in ZK at /configs/brokers/&lt;default&gt;</li>
  *   <li>STATIC_BROKER_CONFIG: properties that broker is started up with, typically from server.properties file</li>
  *   <li>DEFAULT_CONFIG: Default configs defined in KafkaConfig</li>
  * </ol>
  * Log configs use topic config overrides if defined and fallback to broker defaults using the order of precedence above.
  * Topic config overrides may use a different config name from the default broker config.
  * See [[kafka.log.LogConfig#TopicConfigSynonyms]] for the mapping.
  * <p>
  * AdminClient returns all config synonyms in the order of precedence when configs are described with
  * <code>includeSynonyms</code>. In addition to configs that may be defined with the same name at different levels,
  * some configs have additional synonyms.
  * </p>
  * <ul>
  *   <li>Listener configs may be defined using the prefix <tt>listener.name.{listenerName}.{configName}</tt>. These may be
  *       configured as dynamic or static broker configs. Listener configs have higher precedence than the base configs
  *       that don't specify the listener name. Listeners without a listener config use the base config. Base configs
  *       may be defined only as STATIC_BROKER_CONFIG or DEFAULT_CONFIG and cannot be updated dynamically.<li>
  *   <li>Some configs may be defined using multiple properties. For example, <tt>log.roll.ms</tt> and
  *       <tt>log.roll.hours</tt> refer to the same config that may be defined in milliseconds or hours. The order of
  *       precedence of these synonyms is described in the docs of these configs in [[kafka.server.KafkaConfig]].</li>
  * </ul>
  *
  */
object DynamicBrokerConfig {

  private[server] val DynamicSecurityConfigs = SslConfigs.RECONFIGURABLE_CONFIGS.asScala

  // mark 所有可以动态更改的配置都在这里
  val AllDynamicConfigs = DynamicSecurityConfigs ++
    LogCleaner.ReconfigurableConfigs ++
    DynamicLogConfig.ReconfigurableConfigs ++
    DynamicThreadPool.ReconfigurableConfigs ++
    Set(KafkaConfig.MetricReporterClassesProp) ++
    DynamicListenerConfig.ReconfigurableConfigs ++
    SocketServer.ReconfigurableConfigs ++
    ProducerStateManagerConfig.ReconfigurableConfigs

  private val ClusterLevelListenerConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp, KafkaConfig.NumNetworkThreadsProp)

  private val PerBrokerConfigs = (DynamicSecurityConfigs ++ DynamicListenerConfig.ReconfigurableConfigs).diff(
    ClusterLevelListenerConfigs)

  private val ListenerMechanismConfigs = Set(KafkaConfig.SaslJaasConfigProp,
    KafkaConfig.SaslLoginCallbackHandlerClassProp,
    KafkaConfig.SaslLoginClassProp,
    KafkaConfig.SaslServerCallbackHandlerClassProp,
    KafkaConfig.ConnectionsMaxReauthMsProp)

  private val ReloadableFileConfigs = Set(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)

  val ListenerConfigRegex = """listener\.name\.[^.]*\.(.*)""".r

  private val DynamicPasswordConfigs = {
    val passwordConfigs = KafkaConfig.configKeys.filter(_._2.`type` == ConfigDef.Type.PASSWORD).keySet
    AllDynamicConfigs.intersect(passwordConfigs)
  }

  def isPasswordConfig(name: String): Boolean = DynamicBrokerConfig.DynamicPasswordConfigs.exists(name.endsWith)

  def brokerConfigSynonyms(name: String, matchListenerOverride: Boolean): List[String] = {
    name match {
      case KafkaConfig.LogRollTimeMillisProp | KafkaConfig.LogRollTimeHoursProp =>
        List(KafkaConfig.LogRollTimeMillisProp, KafkaConfig.LogRollTimeHoursProp)
      case KafkaConfig.LogRollTimeJitterMillisProp | KafkaConfig.LogRollTimeJitterHoursProp =>
        List(KafkaConfig.LogRollTimeJitterMillisProp, KafkaConfig.LogRollTimeJitterHoursProp)
      case KafkaConfig.LogFlushIntervalMsProp => // LogFlushSchedulerIntervalMsProp is used as default
        List(KafkaConfig.LogFlushIntervalMsProp, KafkaConfig.LogFlushSchedulerIntervalMsProp)
      case KafkaConfig.LogRetentionTimeMillisProp | KafkaConfig.LogRetentionTimeMinutesProp | KafkaConfig.LogRetentionTimeHoursProp =>
        List(KafkaConfig.LogRetentionTimeMillisProp, KafkaConfig.LogRetentionTimeMinutesProp, KafkaConfig.LogRetentionTimeHoursProp)
      case ListenerConfigRegex(baseName) if matchListenerOverride =>
        // `ListenerMechanismConfigs` are specified as listenerPrefix.mechanism.<configName>
        // and other listener configs are specified as listenerPrefix.<configName>
        // Add <configName> as a synonym in both cases.
        val mechanismConfig = ListenerMechanismConfigs.find(baseName.endsWith)
        List(name, mechanismConfig.getOrElse(baseName))
      case _ => List(name)
    }
  }

  def validateConfigs(props: Properties, perBrokerConfig: Boolean): Unit = {
    def checkInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
      if (invalidPropNames.nonEmpty)
        throw new ConfigException(s"$errorMessage: $invalidPropNames")
    }
    checkInvalidProps(nonDynamicConfigs(props), "Cannot update these configs dynamically")
    checkInvalidProps(securityConfigsWithoutListenerPrefix(props),
      "These security configs can be dynamically updated only per-listener using the listener prefix")
    validateConfigTypes(props)
    if (!perBrokerConfig) {
      checkInvalidProps(perBrokerConfigs(props),
        "Cannot update these configs at default cluster level, broker id must be specified")
    }
  }

  private def perBrokerConfigs(props: Properties): Set[String] = {
    val configNames = props.asScala.keySet
    def perBrokerListenerConfig(name: String): Boolean = {
      name match {
        case ListenerConfigRegex(baseName) => !ClusterLevelListenerConfigs.contains(baseName)
        case _ => false
      }
    }
    configNames.intersect(PerBrokerConfigs) ++ configNames.filter(perBrokerListenerConfig)
  }

  /**
   * mark 获取非动态配置的属性名集合。
   *
   * @param props Properties对象，包含所有的配置项。
   * @return 返回一个Set集合，包含非动态配置的属性名。
   */
  private def nonDynamicConfigs(props: Properties): Set[String] = {
    // mark 返回props和 DynamicConfig.Broker.nonDynamicProps的交集
    props.asScala.keySet.intersect(DynamicConfig.Broker.nonDynamicProps)
  }


  private def securityConfigsWithoutListenerPrefix(props: Properties): Set[String] = {
    DynamicSecurityConfigs.filter(props.containsKey)
  }

  /**
   * mark 验证所有的动态配置项的类型是否符合要求。
   * 此函数的目的是对传入的配置属性进行检查，确保它们符合动态配置的预期类型。
   * 它通过匹配特定的键模式（ListenerConfigRegex）来提取基本名称，并将这些配置项以及其它不匹配的配置项
   * 放入一个新的Properties对象中，然后对该新对象进行类型验证。
   *
   * @param props 原始的配置属性对象，可能包含多种类型的配置项。
   */
  private def validateConfigTypes(props: Properties): Unit = {
    // 创建一个新的Properties对象用于存储经过筛选的配置项
    val baseProps = new Properties
    // 遍历原始配置属性，对匹配ListenerConfigRegex的键提取基本名称，并存储到baseProps中，
    // 对于不匹配的键值对，直接存储到baseProps中。
    props.asScala.foreach {
      // mark 如果键匹配 `ListenerConfigRegex` 正则表达式，则提取基本名称并放入 baseProps。
      case (ListenerConfigRegex(baseName), v) => baseProps.put(baseName, v)
      case (k, v) => baseProps.put(k, v)
    }
    // 使用DynamicConfig的Broker实例对baseProps进行类型验证
    DynamicConfig.Broker.validate(baseProps)
  }


  /**
   * mark 将可以动态添加的配置定义添加到给定的配置定义中
   *
   * 此方法遍历KafkaConfig中的所有配置项，并检查哪些配置项是动态配置。如果配置项是动态配置，
   * 则将其添加到ConfigDef中。这样做是为了在运行时能够动态地更新这些特定的配置项，
   * 而不需要重启服务器。
   *
   * @param configDef 配置定义的对象，用于定义和管理配置项。
   */
  private[server] def addDynamicConfigs(configDef: ConfigDef): Unit = {
    // 遍历KafkaConfig中的所有配置项
    // mark KafkaConfig.configKeys 里面是所有的配置定义
    KafkaConfig.configKeys.forKeyValue { (configName, config) =>
      // 检查当前配置项是否是动态配置
      // mark AllDynamicConfigs是动态配置定义
      if (AllDynamicConfigs.contains(configName)) {
        // 如果是动态配置，则在ConfigDef中定义这个配置项
        configDef.define(config.name, config.`type`, config.defaultValue, config.validator,
          config.importance, config.documentation, config.group, config.orderInGroup, config.width,
          config.displayName, config.dependents, config.recommender)
      }
    }
  }


  private[server] def dynamicConfigUpdateModes: util.Map[String, String] = {
    AllDynamicConfigs.map { name =>
      val mode = if (PerBrokerConfigs.contains(name)) "per-broker" else "cluster-wide"
      name -> mode
    }.toMap.asJava
  }

  /**
   * 解析并过滤原始配置属性，去除以特定前缀开头的配置项。
   *
   * 这个方法接受一个包含原始配置的 `Properties` 对象，并返回一个新的 `Properties` 对象。
   * 返回的属性集合中不包含以 `AbstractConfig.CONFIG_PROVIDERS_CONFIG` 前缀开头的配置项。
   *
   * @param propsOriginal 包含原始配置的 `Properties` 对象。
   * @return 一个新的 `Properties` 对象，其中不包含以 `AbstractConfig.CONFIG_PROVIDERS_CONFIG` 前缀开头的配置项。
   */
  private[server] def resolveVariableConfigs(propsOriginal: Properties): Properties = {
    // 创建一个新的 Properties 对象，用于存储过滤后的配置。
    val props = new Properties

    // 创建一个 AbstractConfig 对象来处理原始配置。
    val config = new AbstractConfig(new ConfigDef(), propsOriginal, false)

    // 遍历原始配置项，过滤掉以特定前缀开头的配置项。
    config.originals.forEach { (key, value) =>
      // 检查键是否不以 AbstractConfig.CONFIG_PROVIDERS_CONFIG 开头。
      if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
        // 如果键不以特定前缀开头，则将键值对加入新的 Properties 对象。
        props.put(key, value)
      }
    }

    // 返回过滤后的配置项集合。
    props
  }
}

class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging {
  // mark 静态Broker配置
  private[server] val staticBrokerConfigs = ConfigDef.convertToStringMapWithPasswordValues(kafkaConfig.originalsFromThisConfig).asScala
  // mark 静态集群默认配置
  private[server] val staticDefaultConfigs = ConfigDef.convertToStringMapWithPasswordValues(KafkaConfig.defaultValues.asJava).asScala
  // mark 动态Broker配置
  private val dynamicBrokerConfigs = mutable.Map[String, String]()
  // mark 动态集群默认配置
  private val dynamicDefaultConfigs = mutable.Map[String, String]()

  // Use COWArrayList to prevent concurrent modification exception when an item is added by one thread to these
  // collections, while another thread is iterating over them.
  // mark 用于保存可重新配置的对象
  private[server] val reconfigurables = new CopyOnWriteArrayList[Reconfigurable]()
  private val brokerReconfigurables = new CopyOnWriteArrayList[BrokerReconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var currentConfig: KafkaConfig = _
  private val dynamicConfigPasswordEncoder = if (kafkaConfig.processRoles.isEmpty) {
    maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret)
  } else {
    Some(PasswordEncoder.noop())
  }

  /**
   * mark 动态Broker配置初始化方法。
   *
   * @param zkClientOpt 可选的 KafkaZkClient 实例，用于与 ZooKeeper 进行交互。
   */
  private[server] def initialize(zkClientOpt: Option[KafkaZkClient]): Unit = {
    // mark kafkaConfig.props为配置文件中的初始配置 server.proerties
    currentConfig = new KafkaConfig(kafkaConfig.props, false, None)

    zkClientOpt.foreach { zkClient =>
      // mark 创建 AdminZkClient 实例
      val adminZkClient = new AdminZkClient(zkClient)
      // mark  cluster-wide 类型的参数更新
      // mark 从kafka拉取配置并更新 路径为 /config/brokers/<default> 拉取配置（如果不存在节点则返回空Properties）
      updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default), false)
      // mark per-broker 类型的参数更新
      // mark 从kafka拉取配置并更新 路径为 /config/brokers/<brokerid> 拉取配置（如果不存在节点则返回空Properties）
      val props = adminZkClient.fetchEntityConfig(ConfigType.Broker, kafkaConfig.brokerId.toString)
      val brokerConfig = maybeReEncodePasswords(props, adminZkClient)
      // mark 更新broker配置
      updateBrokerConfig(kafkaConfig.brokerId, brokerConfig)
    }
  }

  /**
   * Clear all cached values. This is used to clear state on broker shutdown to avoid
   * exceptions in tests when broker is restarted. These fields are re-initialized when
   * broker starts up.
   */
  private[server] def clear(): Unit = {
    dynamicBrokerConfigs.clear()
    dynamicDefaultConfigs.clear()
    reconfigurables.clear()
    brokerReconfigurables.clear()
  }

  /**
   * Add reconfigurables to be notified when a dynamic broker config is updated.
   *
   * `Reconfigurable` is the public API used by configurable plugins like metrics reporter
   * and quota callbacks. These are reconfigured before `KafkaConfig` is updated so that
   * the update can be aborted if `reconfigure()` fails with an exception.
   *
   * `BrokerReconfigurable` is used for internal reconfigurable classes. These are
   * reconfigured after `KafkaConfig` is updated so that they can access `KafkaConfig`
   * directly. They are provided both old and new configs.
   */
  def addReconfigurables(kafkaServer: KafkaBroker): Unit = {
    kafkaServer.authorizer match {
      case Some(authz: Reconfigurable) => addReconfigurable(authz)
      case _ =>
    }
    addReconfigurable(kafkaServer.kafkaYammerMetrics)
    addReconfigurable(new DynamicMetricsReporters(kafkaConfig.brokerId, kafkaServer.config, kafkaServer.metrics, kafkaServer.clusterId))
    addReconfigurable(new DynamicClientQuotaCallback(kafkaServer))

    addBrokerReconfigurable(new DynamicThreadPool(kafkaServer))
    addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager, kafkaServer))
    addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
    addBrokerReconfigurable(kafkaServer.socketServer)
    addBrokerReconfigurable(kafkaServer.logManager.producerStateManagerConfig)
  }

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
    reconfigurables.add(reconfigurable)
  }

  def addBrokerReconfigurable(reconfigurable: BrokerReconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs)
    brokerReconfigurables.add(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables.remove(reconfigurable)
  }

  private def verifyReconfigurableConfigs(configNames: Set[String]): Unit = CoreUtils.inWriteLock(lock) {
    val nonDynamic = configNames.filter(DynamicConfig.Broker.nonDynamicProps.contains)
    require(nonDynamic.isEmpty, s"Reconfigurable contains non-dynamic configs $nonDynamic")
  }

  // Visibility for testing
  private[server] def currentKafkaConfig: KafkaConfig = CoreUtils.inReadLock(lock) {
    currentConfig
  }

  private[server] def currentDynamicBrokerConfigs: Map[String, String] = CoreUtils.inReadLock(lock) {
    dynamicBrokerConfigs.clone()
  }

  private[server] def currentDynamicDefaultConfigs: Map[String, String] = CoreUtils.inReadLock(lock) {
    dynamicDefaultConfigs.clone()
  }

  private[server] def updateBrokerConfig(brokerId: Int, persistentProps: Properties, doLog: Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
    try {
      val props = fromPersistentProps(persistentProps, perBrokerConfig = true)
      dynamicBrokerConfigs.clear()
      dynamicBrokerConfigs ++= props.asScala
      updateCurrentConfig(doLog)
    } catch {
      case e: Exception => error(s"Per-broker configs of $brokerId could not be applied: ${persistentProps.keys()}", e)
    }
  }

  /**
   * mark 更新集群层面的默认配置
   *
   * @param persistentProps 需要更新的配置Properties
   * @param doLog           是否记录日志，默认值为 true
   */
  private[server] def updateDefaultConfig(persistentProps: Properties, doLog: Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
    try {
      // mark 对配置进行修正 比如有些不能够自动配置的需要移除
      val props = fromPersistentProps(persistentProps, perBrokerConfig = false)
      // mark 覆盖配置
      dynamicDefaultConfigs.clear()
      dynamicDefaultConfigs ++= props.asScala
      updateCurrentConfig(doLog)
    } catch {
      case e: Exception => error(s"Cluster default configs could not be applied: ${persistentProps.keys()}", e)
    }
  }

  /**
   * All config updates through ZooKeeper are triggered through actual changes in values stored in ZooKeeper.
   * For some configs like SSL keystores and truststores, we also want to reload the store if it was modified
   * in-place, even though the actual value of the file path and password haven't changed. This scenario alone
   * is handled here when a config update request using admin client is processed by ZkAdminManager. If any of
   * the SSL configs have changed, then the update will not be done here, but will be handled later when ZK
   * changes are processed. At the moment, only listener configs are considered for reloading.
   */
  private[server] def reloadUpdatedFilesWithoutConfigChange(newProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables.asScala
      .filter(reconfigurable => ReloadableFileConfigs.exists(reconfigurable.reconfigurableConfigs.contains))
      .foreach {
        case reconfigurable: ListenerReconfigurable =>
          val kafkaProps = validatedKafkaProps(newProps, perBrokerConfig = true)
          val newConfig = new KafkaConfig(kafkaProps.asJava, false, None)
          processListenerReconfigurable(reconfigurable, newConfig, Collections.emptyMap(), validateOnly = false, reloadOnly = true)
        case reconfigurable =>
          trace(s"Files will not be reloaded without config change for $reconfigurable")
      }
  }

  private def maybeCreatePasswordEncoder(secret: Option[Password]): Option[PasswordEncoder] = {
   secret.map { secret =>
     PasswordEncoder.encrypting(secret,
        kafkaConfig.passwordEncoderKeyFactoryAlgorithm,
        kafkaConfig.passwordEncoderCipherAlgorithm,
        kafkaConfig.passwordEncoderKeyLength,
        kafkaConfig.passwordEncoderIterations)
    }
  }

  private def passwordEncoder: PasswordEncoder = {
    dynamicConfigPasswordEncoder.getOrElse(throw new ConfigException("Password encoder secret not configured"))
  }

  private[server] def toPersistentProps(configProps: Properties, perBrokerConfig: Boolean): Properties = {
    val props = configProps.clone().asInstanceOf[Properties]

    def encodePassword(configName: String, value: String): Unit = {
      if (value != null) {
        if (!perBrokerConfig)
          throw new ConfigException("Password config can be defined only at broker level")
        props.setProperty(configName, passwordEncoder.encode(new Password(value)))
      }
    }
    configProps.asScala.forKeyValue { (name, value) =>
      if (isPasswordConfig(name))
        encodePassword(name, value)
    }
    props
  }

  /**
   * 从持久化属性生成配置
   * 该方法从持久化属性中生成配置，并移除无效的配置项。
   * 它处理动态和安全配置，并对密码进行解码。
   *
   * @param persistentProps 持久化属性
   * @param perBrokerConfig 是否为每个代理配置
   * @return 处理后的配置属性
   */
  private[server] def fromPersistentProps(persistentProps: Properties, perBrokerConfig: Boolean): Properties = {
    val props = persistentProps.clone().asInstanceOf[Properties]

    // mark 移除 `props` 中所有无效的配置项 perBrokerConfig为true表示broker 为false表示cluster
    removeInvalidConfigs(props, perBrokerConfig)

    /**
     * mark 移除无效的属性。
     *
     * 本函数用于从一个集合中移除指定的无效属性名，并记录一个错误信息。如果传入的无效属性名集合不为空，
     * 则遍历该集合，从一个名为`props`的存储结构中移除这些属性。同时，使用传入的错误信息和移除的属性名
     * 组合成一条具体的错误信息，并记录下来。
     *
     * @param invalidPropNames 一个包含无效属性名的集合。
     * @param errorMessage     错误信息的模板，用于在移除无效属性时记录错误。
     */
    def removeInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
      // 当无效属性名集合不为空时，执行属性移除和错误记录操作
      if (invalidPropNames.nonEmpty) {
        // 遍历无效属性名集合，从`props`中移除这些属性
        invalidPropNames.foreach(props.remove)
        // 使用错误信息模板和实际的无效属性名生成并记录错误信息
        error(s"$errorMessage: $invalidPropNames")
      }
    }

    // mark ZooKeeper 中配置的非动态配置项将被忽略
    removeInvalidProps(nonDynamicConfigs(props), "Non-dynamic configs configured in ZooKeeper will be ignored")
    // mark 安全配置仅能通过监听器前缀动态更新，基础配置将被忽略(可以更新监听器名称 但是相关的安全配置都固定)
    removeInvalidProps(securityConfigsWithoutListenerPrefix(props),
      "Security configs can be dynamically updated only using listener prefix, base configs will be ignored")
    // mark 默认集群级别定义的每代理（per-broker）配置将被忽略
    if (!perBrokerConfig)
      removeInvalidProps(perBrokerConfigs(props), "Per-broker configs defined at default cluster level will be ignored")

    def decodePassword(configName: String, value: String): Unit = {
      if (value != null) {
        try {
          props.setProperty(configName, passwordEncoder.decode(value).value)
        } catch {
          case e: Exception =>
            error(s"Dynamic password config $configName could not be decoded, ignoring.", e)
            props.remove(configName)
        }
      }
    }

    props.asScala.forKeyValue { (name, value) =>
      if (isPasswordConfig(name))
        decodePassword(name, value)
    }

    props
  }

  // If the secret has changed, password.encoder.old.secret contains the old secret that was used
  // to encode the configs in ZK. Decode passwords using the old secret and update ZK with values
  // encoded using the current secret. Ignore any errors during decoding since old secret may not
  // have been removed during broker restart.
  private def maybeReEncodePasswords(persistentProps: Properties, adminZkClient: AdminZkClient): Properties = {
    val props = persistentProps.clone().asInstanceOf[Properties]
    if (props.asScala.keySet.exists(isPasswordConfig)) {
      maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderOldSecret).foreach { passwordDecoder =>
        persistentProps.asScala.forKeyValue { (configName, value) =>
          if (isPasswordConfig(configName) && value != null) {
            val decoded = try {
              Some(passwordDecoder.decode(value).value)
            } catch {
              case _: Exception =>
                debug(s"Dynamic password config $configName could not be decoded using old secret, new secret will be used.")
                None
            }
            decoded.foreach(value => props.put(configName, passwordEncoder.encode(new Password(value))))
          }
        }
        adminZkClient.changeBrokerConfig(Some(kafkaConfig.brokerId), props)
      }
    }
    props
  }

  /**
   * Validate the provided configs `propsOverride` and return the full Kafka configs with
   * the configured defaults and these overrides.
   *
   * Note: The caller must acquire the read or write lock before invoking this method.
   */
  private def validatedKafkaProps(propsOverride: Properties, perBrokerConfig: Boolean): Map[String, String] = {
    val propsResolved = DynamicBrokerConfig.resolveVariableConfigs(propsOverride)
    validateConfigs(propsResolved, perBrokerConfig)
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    if (perBrokerConfig) {
      overrideProps(newProps, dynamicDefaultConfigs)
      overrideProps(newProps, propsResolved.asScala)
    } else {
      overrideProps(newProps, propsResolved.asScala)
      overrideProps(newProps, dynamicBrokerConfigs)
    }
    newProps
  }

  private[server] def validate(props: Properties, perBrokerConfig: Boolean): Unit = CoreUtils.inReadLock(lock) {
    val newProps = validatedKafkaProps(props, perBrokerConfig)
    processReconfiguration(newProps, validateOnly = true)
  }

  /**
   * mark 验证并且移除无效的配置项。
   *
   * 此方法尝试验证给定配置项的有效性。如果验证失败，它将识别并移除导致验证失败的配置项。
   * 这对于确保配置的正确性和系统的稳定性至关重要。
   *
   * @param props           配置项的集合，可能包含无效的配置项。
   * @param perBrokerConfig 指示这些配置项是否针对每个Broker，用于提供更具体的错误上下文信息。
   */
  private def removeInvalidConfigs(props: Properties, perBrokerConfig: Boolean): Unit = {
    try {
      // mark 先整体验证一遍 如果报错则再每个属性一次过一遍
      validateConfigTypes(props)
      // 如果验证成功，无需进一步处理，直接返回Scala映射视图。
      props.asScala
    } catch {
      case e: Exception =>
        // mark 筛选出导致验证失败的配置项。
        val invalidProps = props.asScala.filter { case (k, v) =>
          val props1 = new Properties
          props1.put(k, v)
          try {
            validateConfigTypes(props1)
            false // mark 验证成功，此项不是无效配置。
          } catch {
            case _: Exception => true // mark 验证失败，此项是无效配置。 从props中移除
          }
        }
        // mark 从原始配置中移除无效配置项。
        invalidProps.keys.foreach(props.remove)
        // 根据perBrokerConfig参数确定配置源的类型。
        val configSource = if (perBrokerConfig) "broker" else "default cluster"
        // 记录移除无效配置项的日志，包括哪些配置项被移除以及相关的异常信息。
        error(s"Dynamic $configSource config contains invalid values in: ${invalidProps.keys}, these configs will be ignored", e)
    }
  }


  private[server] def maybeReconfigure(reconfigurable: Reconfigurable, oldConfig: KafkaConfig, newConfig: util.Map[String, _]): Unit = {
    if (reconfigurable.reconfigurableConfigs.asScala.exists(key => oldConfig.originals.get(key) != newConfig.get(key)))
      reconfigurable.reconfigure(newConfig)
  }

  /**
   * mark 返回新旧配置中更新的配置 已经被删除的配置
   *
   * @param newProps     新配置属性的映射
   * @param currentProps 当前配置属性的映射
   * @return 包含更改配置的映射和删除键的集合
   */
  private def updatedConfigs(newProps: java.util.Map[String, _], currentProps: java.util.Map[String, _]): (mutable.Map[String, _], Set[String]) = {
    // mark 过滤出更新的配置
    val changeMap = newProps.asScala.filter {
      case (k, v) => v != currentProps.get(k)
    }
    // mark 过滤出已经删除的配置
    val deletedKeySet = currentProps.asScala.filter {
      case (k, _) => !newProps.containsKey(k)
    }.keySet
    (changeMap, deletedKeySet)
  }
  /**
   * 使用 `propsOverride` 中的新值更新 `props` 中的值。
   * 已更新配置的同义词将从 `props` 中移除，以确保应用具有更高优先级的配置。
   * 例如，如果 `log.roll.ms` 在 server.properties 中定义，且 `log.roll.hours` 被动态配置，
   * 则将使用动态配置中的 `log.roll.hours`，并且 `log.roll.ms` 将从 `props` 中移除（即使 `log.roll.hours` 的优先级低于 `log.roll.ms`）。
   *
   * @param props         需要被覆盖更新的配置映射
   * @param propsOverride 提供新值的配置映射
   */
  private def overrideProps(props: mutable.Map[String, String], propsOverride: mutable.Map[String, String]): Unit = {
    propsOverride.forKeyValue { (k, v) =>
      // 移除 `k` 的同义词，以确保应用正确的优先级。但禁用 `matchListenerOverride`
      // 以避免移除与监听器配置对应的基础配置。基础配置不应被移除，因为其他监听器可能会使用它们。
      // 保留基础配置是可以的，因为基础配置不能被动态更新，且监听器特定的配置具有更高优先级。
      brokerConfigSynonyms(k, matchListenerOverride = false).foreach(props.remove)
      props.put(k, v)
    }
  }

  /**
   * mark 动态配置覆盖静态配置
   * 更新当前配置
   * 该方法更新当前的代理配置，并应用动态配置覆盖静态配置。
   * 它处理配置重新加载，并在必要时更新相关组件。
   *
   * @param doLog 是否记录日志
   */
  private def updateCurrentConfig(doLog: Boolean): Unit = {
    val newProps = mutable.Map[String, String]()

    // mark 将 staticBrokerConfigs (服务启动配置文件中的配置) 标记为新配置
    newProps ++= staticBrokerConfigs
    // mark 用 dynamicDefaultConfigs 覆盖 newProps 中的相应配置
    overrideProps(newProps, dynamicDefaultConfigs)
    // 用 dynamicBrokerConfigs 覆盖 newProps 中的相应配置
    overrideProps(newProps, dynamicBrokerConfigs)

    val oldConfig = currentConfig
    val (newConfig, brokerReconfigurablesToUpdate) = processReconfiguration(newProps, validateOnly = false, doLog)

    // mark  scala中ne表示 not equal
    if (newConfig ne currentConfig) {
      currentConfig = newConfig
      // mark 更新currentConfig
      kafkaConfig.updateCurrentConfig(newConfig)

      // 在当前配置更新后处理 BrokerReconfigurable 更新
      brokerReconfigurablesToUpdate.foreach(_.reconfigure(oldConfig, newConfig))
    }
  }

  /**
   * 处理重新配置
   * 该方法根据新配置属性生成新的 KafkaConfig 对象，并处理与配置变化相关的组件重新配置。
   * 它还验证并更新需要重新配置的代理配置项。
   *
   * @param newProps     新的配置属性映射
   * @param validateOnly 是否仅进行验证而不实际更新
   * @param doLog        是否记录日志，默认值为 false
   * @return 新的 KafkaConfig 对象和需要更新的 BrokerReconfigurable 列表
   */
  private def processReconfiguration(newProps: Map[String, String], validateOnly: Boolean, doLog: Boolean = false): (KafkaConfig, List[BrokerReconfigurable]) = {
    // mark 生成新的KafkaConfig
    val newConfig = new KafkaConfig(newProps.asJava, doLog, None)

    val (changeMap, deletedKeySet) = updatedConfigs(newConfig.originalsFromThisConfig, currentConfig.originals)

    if (changeMap.nonEmpty || deletedKeySet.nonEmpty) {
      try {
        val customConfigs = new util.HashMap[String, Object](newConfig.originalsFromThisConfig) // 非 Kafka 配置
        newConfig.valuesFromThisConfig.keySet.forEach(k => customConfigs.remove(k))

        reconfigurables.forEach {
          // mark 处理监听器相关配置
          case listenerReconfigurable: ListenerReconfigurable =>
            processListenerReconfigurable(listenerReconfigurable, newConfig, customConfigs, validateOnly, reloadOnly = false)
          case reconfigurable =>
            if (needsReconfiguration(reconfigurable.reconfigurableConfigs, changeMap.keySet, deletedKeySet))
              processReconfigurable(reconfigurable, changeMap.keySet, newConfig.valuesFromThisConfig, customConfigs, validateOnly)
        }

        // 在配置更新后处理 BrokerReconfigurable 更新。这里只进行验证。
        val brokerReconfigurablesToUpdate = mutable.Buffer[BrokerReconfigurable]()
        brokerReconfigurables.forEach { reconfigurable =>
          if (needsReconfiguration(reconfigurable.reconfigurableConfigs.asJava, changeMap.keySet, deletedKeySet)) {
            reconfigurable.validateReconfiguration(newConfig)
            if (!validateOnly)
              brokerReconfigurablesToUpdate += reconfigurable
          }
        }
        (newConfig, brokerReconfigurablesToUpdate.toList)
      } catch {
        case e: Exception =>
          if (!validateOnly)
            error(s"无法使用配置更新代理配置: " +
              s"${ConfigUtils.configMapToRedactedString(newConfig.originalsFromThisConfig, KafkaConfig.configDef)}", e)
          throw new ConfigException("无效的动态配置", e)
      }
    } else {
      (currentConfig, List.empty)
    }
  }

  private def needsReconfiguration(reconfigurableConfigs: util.Set[String], updatedKeys: Set[String], deletedKeys: Set[String]): Boolean = {
    reconfigurableConfigs.asScala.intersect(updatedKeys).nonEmpty ||
      reconfigurableConfigs.asScala.intersect(deletedKeys).nonEmpty
  }

  /**
   * 处理 ListenerReconfigurable 对象的重新配置
   * 该方法根据新的 Kafka 配置和自定义配置，处理特定监听器的重新配置或验证。
   *
   * @param listenerReconfigurable 要重新配置的 ListenerReconfigurable 对象
   * @param newConfig              新的 Kafka 配置
   * @param customConfigs          自定义配置映射
   * @param validateOnly           是否仅进行验证而不实际更新
   * @param reloadOnly             是否仅在配置未改变时重新加载
   */
  private def processListenerReconfigurable(listenerReconfigurable: ListenerReconfigurable,
                                            newConfig: KafkaConfig,
                                            customConfigs: util.Map[String, Object],
                                            validateOnly: Boolean,
                                            reloadOnly:  Boolean): Unit = {
    val listenerName = listenerReconfigurable.listenerName
    val oldValues = currentConfig.valuesWithPrefixOverride(listenerName.configPrefix)
    val newValues = newConfig.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    val (changeMap, deletedKeys) = updatedConfigs(newValues, oldValues)
    val updatedKeys = changeMap.keySet
    val configsChanged = needsReconfiguration(listenerReconfigurable.reconfigurableConfigs, updatedKeys, deletedKeys)

    // 如果 `reloadOnly` 为 true，在配置未改变时重新配置。否则，在配置改变时重新配置。
    if (reloadOnly != configsChanged)
      processReconfigurable(listenerReconfigurable, updatedKeys, newValues, customConfigs, validateOnly)
  }

  private def processReconfigurable(reconfigurable: Reconfigurable,
                                    updatedConfigNames: Set[String],
                                    allNewConfigs: util.Map[String, _],
                                    newCustomConfigs: util.Map[String, Object],
                                    validateOnly: Boolean): Unit = {
    val newConfigs = new util.HashMap[String, Object]
    allNewConfigs.forEach((k, v) => newConfigs.put(k, v.asInstanceOf[AnyRef]))
    newConfigs.putAll(newCustomConfigs)
    try {
      reconfigurable.validateReconfiguration(newConfigs)
    } catch {
      case e: ConfigException => throw e
      case _: Exception =>
        throw new ConfigException(s"Validation of dynamic config update of $updatedConfigNames failed with class ${reconfigurable.getClass}")
    }

    if (!validateOnly) {
      info(s"Reconfiguring $reconfigurable, updated configs: $updatedConfigNames " +
           s"custom configs: ${ConfigUtils.configMapToRedactedString(newCustomConfigs, KafkaConfig.configDef)}")
      reconfigurable.reconfigure(newConfigs)
    }
  }
}

trait BrokerReconfigurable {

  def reconfigurableConfigs: Set[String]

  def validateReconfiguration(newConfig: KafkaConfig): Unit

  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit
}

object DynamicLogConfig {
  // Exclude message.format.version for now since we need to check that the version
  // is supported on all brokers in the cluster.
  @nowarn("cat=deprecation")
  val ExcludedConfigs = Set(KafkaConfig.LogMessageFormatVersionProp)

  val ReconfigurableConfigs = LogConfig.TopicConfigSynonyms.values.toSet -- ExcludedConfigs
  val KafkaConfigToLogConfigName = LogConfig.TopicConfigSynonyms.map { case (k, v) => (v, k) }
}

class DynamicLogConfig(logManager: LogManager, server: KafkaBroker) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicLogConfig.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    // For update of topic config overrides, only config names and types are validated
    // Names and types have already been validated. For consistency with topic config
    // validation, no additional validation is performed.
  }

  private def updateLogsConfig(newBrokerDefaults: Map[String, Object]): Unit = {
    logManager.brokerConfigUpdated()
    logManager.allLogs.foreach { log =>
      val props = mutable.Map.empty[Any, Any]
      props ++= newBrokerDefaults
      props ++= log.config.originals.asScala.filter { case (k, _) =>
        log.config.overriddenConfigs.contains(k)
      }

      val logConfig = LogConfig(props.asJava, log.config.overriddenConfigs)
      log.updateConfig(logConfig)
    }
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val originalLogConfig = logManager.currentDefaultConfig
    val originalUncleanLeaderElectionEnable = originalLogConfig.uncleanLeaderElectionEnable
    val newBrokerDefaults = new util.HashMap[String, Object](originalLogConfig.originals)
    newConfig.valuesFromThisConfig.forEach { (k, v) =>
      if (DynamicLogConfig.ReconfigurableConfigs.contains(k)) {
        DynamicLogConfig.KafkaConfigToLogConfigName.get(k).foreach { configName =>
          if (v == null)
             newBrokerDefaults.remove(configName)
          else
            newBrokerDefaults.put(configName, v.asInstanceOf[AnyRef])
        }
      }
    }

    logManager.reconfigureDefaultLogConfig(LogConfig(newBrokerDefaults))

    updateLogsConfig(newBrokerDefaults.asScala)

    if (logManager.currentDefaultConfig.uncleanLeaderElectionEnable && !originalUncleanLeaderElectionEnable) {
      server match {
        case kafkaServer: KafkaServer => kafkaServer.kafkaController.enableDefaultUncleanLeaderElection()
        case _ =>
      }
    }
  }
}

object DynamicThreadPool {
  val ReconfigurableConfigs = Set(
    KafkaConfig.NumIoThreadsProp,
    KafkaConfig.NumReplicaFetchersProp,
    KafkaConfig.NumRecoveryThreadsPerDataDirProp,
    KafkaConfig.BackgroundThreadsProp)
}

class DynamicThreadPool(server: KafkaBroker) extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicThreadPool.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    newConfig.values.forEach { (k, v) =>
      if (DynamicThreadPool.ReconfigurableConfigs.contains(k)) {
        val newValue = v.asInstanceOf[Int]
        val oldValue = currentValue(k)
        if (newValue != oldValue) {
          val errorMsg = s"Dynamic thread count update validation failed for $k=$v"
          if (newValue <= 0)
            throw new ConfigException(s"$errorMsg, value should be at least 1")
          if (newValue < oldValue / 2)
            throw new ConfigException(s"$errorMsg, value should be at least half the current value $oldValue")
          if (newValue > oldValue * 2)
            throw new ConfigException(s"$errorMsg, value should not be greater than double the current value $oldValue")
        }
      }
    }
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (newConfig.numIoThreads != oldConfig.numIoThreads)
      server.dataPlaneRequestHandlerPool.resizeThreadPool(newConfig.numIoThreads)
    if (newConfig.numReplicaFetchers != oldConfig.numReplicaFetchers)
      server.replicaManager.resizeFetcherThreadPool(newConfig.numReplicaFetchers)
    if (newConfig.numRecoveryThreadsPerDataDir != oldConfig.numRecoveryThreadsPerDataDir)
      server.logManager.resizeRecoveryThreadPool(newConfig.numRecoveryThreadsPerDataDir)
    if (newConfig.backgroundThreads != oldConfig.backgroundThreads)
      server.kafkaScheduler.resizeThreadPool(newConfig.backgroundThreads)
  }

  private def currentValue(name: String): Int = {
    name match {
      case KafkaConfig.NumIoThreadsProp => server.config.numIoThreads
      case KafkaConfig.NumReplicaFetchersProp => server.config.numReplicaFetchers
      case KafkaConfig.NumRecoveryThreadsPerDataDirProp => server.config.numRecoveryThreadsPerDataDir
      case KafkaConfig.BackgroundThreadsProp => server.config.backgroundThreads
      case n => throw new IllegalStateException(s"Unexpected config $n")
    }
  }
}

class DynamicMetricsReporters(brokerId: Int, config: KafkaConfig, metrics: Metrics, clusterId: String) extends Reconfigurable {
  private val reporterState = new DynamicMetricReporterState(brokerId, config, metrics, clusterId)
  private[server] val currentReporters = reporterState.currentReporters
  private val dynamicConfig = reporterState.dynamicConfig

  private def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] =
    reporterState.metricsReporterClasses(configs)

  private def createReporters(reporterClasses: util.List[String], updatedConfigs: util.Map[String, _]): Unit =
    reporterState.createReporters(reporterClasses, updatedConfigs)

  private def removeReporter(className: String): Unit = reporterState.removeReporter(className)

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    val configs = new util.HashSet[String]()
    configs.add(KafkaConfig.MetricReporterClassesProp)
    currentReporters.values.foreach {
      case reporter: Reconfigurable => configs.addAll(reporter.reconfigurableConfigs)
      case _ =>
    }
    configs
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    val updatedMetricsReporters = metricsReporterClasses(configs)

    // Ensure all the reporter classes can be loaded and have a default constructor
    updatedMetricsReporters.foreach { className =>
      val clazz = Utils.loadClass(className, classOf[MetricsReporter])
      clazz.getConstructor()
    }

    // Validate the new configuration using every reconfigurable reporter instance that is not being deleted
    currentReporters.values.foreach {
      case reporter: Reconfigurable =>
        if (updatedMetricsReporters.contains(reporter.getClass.getName))
          reporter.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val updatedMetricsReporters = metricsReporterClasses(configs)
    val deleted = currentReporters.keySet.toSet -- updatedMetricsReporters
    deleted.foreach(removeReporter)
    currentReporters.values.foreach {
      case reporter: Reconfigurable => dynamicConfig.maybeReconfigure(reporter, dynamicConfig.currentKafkaConfig, configs)
      case _ =>
    }
    val added = updatedMetricsReporters.filterNot(currentReporters.keySet)
    createReporters(added.asJava, configs)
  }
}

class DynamicMetricReporterState(brokerId: Int, config: KafkaConfig, metrics: Metrics, clusterId: String) {
  private[server] val dynamicConfig = config.dynamicConfig
  private val propsOverride = Map[String, AnyRef](KafkaConfig.BrokerIdProp -> brokerId.toString)
  private[server] val currentReporters = mutable.Map[String, MetricsReporter]()
  createReporters(config, clusterId, metricsReporterClasses(dynamicConfig.currentKafkaConfig.values()).asJava,
    Collections.emptyMap[String, Object])

  private[server] def createReporters(reporterClasses: util.List[String],
                                      updatedConfigs: util.Map[String, _]): Unit = {
    createReporters(config, clusterId, reporterClasses, updatedConfigs)
  }

  private def createReporters(config: KafkaConfig,
                              clusterId: String,
                              reporterClasses: util.List[String],
                              updatedConfigs: util.Map[String, _]): Unit = {
    val props = new util.HashMap[String, AnyRef]
    updatedConfigs.forEach((k, v) => props.put(k, v.asInstanceOf[AnyRef]))
    propsOverride.forKeyValue((k, v) => props.put(k, v))
    val reporters = dynamicConfig.currentKafkaConfig.getConfiguredInstances(reporterClasses, classOf[MetricsReporter], props)

    // Call notifyMetricsReporters first to satisfy the contract for MetricsReporter.contextChange,
    // which provides that MetricsReporter.contextChange must be called before the first call to MetricsReporter.init.
    // The first call to MetricsReporter.init is done when we call metrics.addReporter below.
    KafkaBroker.notifyMetricsReporters(clusterId, config, reporters.asScala)
    reporters.forEach { reporter =>
      metrics.addReporter(reporter)
      currentReporters += reporter.getClass.getName -> reporter
    }
    KafkaBroker.notifyClusterListeners(clusterId, reporters.asScala)
  }

  private[server] def removeReporter(className: String): Unit = {
    currentReporters.remove(className).foreach(metrics.removeReporter)
  }

  @nowarn("cat=deprecation")
  private[server] def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] = {
    val reporters = mutable.Buffer[String]()
    reporters ++= configs.get(KafkaConfig.MetricReporterClassesProp).asInstanceOf[util.List[String]].asScala
    if (configs.get(KafkaConfig.AutoIncludeJmxReporterProp).asInstanceOf[Boolean] &&
        !reporters.contains(classOf[JmxReporter].getName)) {
      reporters += classOf[JmxReporter].getName
    }
    reporters
  }
}

object DynamicListenerConfig {
  /**
   * The set of configurations which the DynamicListenerConfig object listens for. Many of
   * these are also monitored by other objects such as ChannelBuilders and SocketServers.
   */
  val ReconfigurableConfigs = Set(
    // Listener configs
    KafkaConfig.AdvertisedListenersProp,
    KafkaConfig.ListenersProp,
    KafkaConfig.ListenerSecurityProtocolMapProp,

    // SSL configs
    KafkaConfig.PrincipalBuilderClassProp,
    KafkaConfig.SslProtocolProp,
    KafkaConfig.SslProviderProp,
    KafkaConfig.SslCipherSuitesProp,
    KafkaConfig.SslEnabledProtocolsProp,
    KafkaConfig.SslKeystoreTypeProp,
    KafkaConfig.SslKeystoreLocationProp,
    KafkaConfig.SslKeystorePasswordProp,
    KafkaConfig.SslKeyPasswordProp,
    KafkaConfig.SslTruststoreTypeProp,
    KafkaConfig.SslTruststoreLocationProp,
    KafkaConfig.SslTruststorePasswordProp,
    KafkaConfig.SslKeyManagerAlgorithmProp,
    KafkaConfig.SslTrustManagerAlgorithmProp,
    KafkaConfig.SslEndpointIdentificationAlgorithmProp,
    KafkaConfig.SslSecureRandomImplementationProp,
    KafkaConfig.SslClientAuthProp,
    KafkaConfig.SslEngineFactoryClassProp,

    // SASL configs
    KafkaConfig.SaslMechanismInterBrokerProtocolProp,
    KafkaConfig.SaslJaasConfigProp,
    KafkaConfig.SaslEnabledMechanismsProp,
    KafkaConfig.SaslKerberosServiceNameProp,
    KafkaConfig.SaslKerberosKinitCmdProp,
    KafkaConfig.SaslKerberosTicketRenewWindowFactorProp,
    KafkaConfig.SaslKerberosTicketRenewJitterProp,
    KafkaConfig.SaslKerberosMinTimeBeforeReloginProp,
    KafkaConfig.SaslKerberosPrincipalToLocalRulesProp,
    KafkaConfig.SaslLoginRefreshWindowFactorProp,
    KafkaConfig.SaslLoginRefreshWindowJitterProp,
    KafkaConfig.SaslLoginRefreshMinPeriodSecondsProp,
    KafkaConfig.SaslLoginRefreshBufferSecondsProp,

    // Connection limit configs
    KafkaConfig.MaxConnectionsProp,
    KafkaConfig.MaxConnectionCreationRateProp,

    // Network threads
    KafkaConfig.NumNetworkThreadsProp
  )
}

class DynamicClientQuotaCallback(server: KafkaBroker) extends Reconfigurable {

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    val configs = new util.HashSet[String]()
    server.quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable => configs.addAll(callback.reconfigurableConfigs)
      case _ =>
    }
    configs
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    server.quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable => callback.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val config = server.config
    server.quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable =>
        config.dynamicConfig.maybeReconfigure(callback, config.dynamicConfig.currentKafkaConfig, configs)
        true
      case _ => false
    }
  }
}

class DynamicListenerConfig(server: KafkaBroker) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicListenerConfig.ReconfigurableConfigs
  }

  private def listenerRegistrationsAltered(
    oldAdvertisedListeners: Map[ListenerName, EndPoint],
    newAdvertisedListeners: Map[ListenerName, EndPoint]
  ): Boolean = {
    if (oldAdvertisedListeners.size != newAdvertisedListeners.size) return true
    oldAdvertisedListeners.forKeyValue {
      case (oldListenerName, oldEndpoint) =>
        newAdvertisedListeners.get(oldListenerName) match {
          case None => return true
          case Some(newEndpoint) => if (!newEndpoint.equals(oldEndpoint)) {
            return true
          }
        }
    }
    false
  }

  private def verifyListenerRegistrationAlterationSupported(): Unit = {
    if (!server.config.requiresZookeeper) {
      throw new ConfigException("Advertised listeners cannot be altered when using a " +
        "Raft-based metadata quorum.")
    }
  }

  def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val oldConfig = server.config
    val newListeners = listenersToMap(newConfig.listeners)
    val newAdvertisedListeners = listenersToMap(newConfig.effectiveAdvertisedListeners)
    val oldListeners = listenersToMap(oldConfig.listeners)
    if (!newAdvertisedListeners.keySet.subsetOf(newListeners.keySet))
      throw new ConfigException(s"Advertised listeners '$newAdvertisedListeners' must be a subset of listeners '$newListeners'")
    if (!newListeners.keySet.subsetOf(newConfig.effectiveListenerSecurityProtocolMap.keySet))
      throw new ConfigException(s"Listeners '$newListeners' must be subset of listener map '${newConfig.effectiveListenerSecurityProtocolMap}'")
    newListeners.keySet.intersect(oldListeners.keySet).foreach { listenerName =>
      def immutableListenerConfigs(kafkaConfig: KafkaConfig, prefix: String): Map[String, AnyRef] = {
        kafkaConfig.originalsWithPrefix(prefix, true).asScala.filter { case (key, _) =>
          // skip the reconfigurable configs
          !DynamicSecurityConfigs.contains(key) && !SocketServer.ListenerReconfigurableConfigs.contains(key) && !DataPlaneAcceptor.ListenerReconfigurableConfigs.contains(key)
        }
      }
      if (immutableListenerConfigs(newConfig, listenerName.configPrefix) != immutableListenerConfigs(oldConfig, listenerName.configPrefix))
        throw new ConfigException(s"Configs cannot be updated dynamically for existing listener $listenerName, " +
          "restart broker or create a new listener for update")
      if (oldConfig.effectiveListenerSecurityProtocolMap(listenerName) != newConfig.effectiveListenerSecurityProtocolMap(listenerName))
        throw new ConfigException(s"Security protocol cannot be updated for existing listener $listenerName")
    }
    if (!newAdvertisedListeners.contains(newConfig.interBrokerListenerName))
      throw new ConfigException(s"Advertised listener must be specified for inter-broker listener ${newConfig.interBrokerListenerName}")

    // Currently, we do not support adding or removing listeners when in KRaft mode.
    // However, we support changing other listener configurations (max connections, etc.)
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedListeners),
        listenersToMap(newConfig.effectiveAdvertisedListeners))) {
      verifyListenerRegistrationAlterationSupported()
    }
  }

  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val newListeners = newConfig.listeners
    val newListenerMap = listenersToMap(newListeners)
    val oldListeners = oldConfig.listeners
    val oldListenerMap = listenersToMap(oldListeners)
    val listenersRemoved = oldListeners.filterNot(e => newListenerMap.contains(e.listenerName))
    val listenersAdded = newListeners.filterNot(e => oldListenerMap.contains(e.listenerName))
    if (listenersRemoved.nonEmpty || listenersAdded.nonEmpty) {
      LoginManager.closeAll() // Clear SASL login cache to force re-login
      if (listenersRemoved.nonEmpty) server.socketServer.removeListeners(listenersRemoved)
      if (listenersAdded.nonEmpty) server.socketServer.addListeners(listenersAdded)
    }
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedListeners),
        listenersToMap(newConfig.effectiveAdvertisedListeners))) {
      verifyListenerRegistrationAlterationSupported()
      server match {
        case kafkaServer: KafkaServer => kafkaServer.kafkaController.updateBrokerInfo(kafkaServer.createBrokerInfo)
        case _ => throw new RuntimeException("Unable to handle non-kafkaServer")
      }
    }
  }

  private def listenersToMap(listeners: Seq[EndPoint]): Map[ListenerName, EndPoint] =
    listeners.map(e => (e.listenerName, e)).toMap

}
