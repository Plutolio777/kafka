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
import java.util.concurrent.TimeUnit
import java.util.{Collections, Locale, Properties}
import kafka.cluster.EndPoint
import kafka.coordinator.group.OffsetConfig
import kafka.coordinator.transaction.{TransactionLog, TransactionStateManager}
import kafka.log.LogConfig
import kafka.log.LogConfig.MessageFormatVersion
import kafka.message.{BrokerCompressionCodec, CompressionCodec, ProducerCompressionCodec, ZStdCompressionCodec}
import kafka.security.authorizer.AuthorizerUtils
import kafka.server.KafkaConfig.{ControllerListenerNamesProp, ListenerSecurityProtocolMapProp}
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole, ProcessRole}
import kafka.utils.CoreUtils.parseCsvList
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, SaslConfigs, SecurityConfig, SslClientAuth, SslConfigs, TopicConfig}
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{LegacyRecord, Records, TimestampType}
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{MetadataVersion, MetadataVersionValidator}
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.zookeeper.client.ZKClientConfig

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}

object Defaults {
  /** ********* Zookeeper Configuration ***********/
  val ZkSessionTimeoutMs = 18000
  val ZkEnableSecureAcls = false
  val ZkMaxInFlightRequests = 10
  val ZkSslClientEnable = false
  val ZkSslProtocol = "TLSv1.2"
  val ZkSslEndpointIdentificationAlgorithm = "HTTPS"
  val ZkSslCrlEnable = false
  val ZkSslOcspEnable = false

  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnable = true
  val MaxReservedBrokerId = 1000
  val BrokerId = -1
  val MessageMaxBytes = 1024 * 1024 + Records.LOG_OVERHEAD
  val NumNetworkThreads = 3
  val NumIoThreads = 8
  val BackgroundThreads = 10
  val QueuedMaxRequests = 500
  val QueuedMaxRequestBytes = -1
  val InitialBrokerRegistrationTimeoutMs = 60000
  val BrokerHeartbeatIntervalMs = 2000
  val BrokerSessionTimeoutMs = 9000
  val MetadataSnapshotMaxNewRecordBytes = 20 * 1024 * 1024
  val MetadataSnapshotMaxIntervalMs = TimeUnit.HOURS.toMillis(1);
  val MetadataMaxIdleIntervalMs = 500
  val MetadataMaxRetentionBytes = 100 * 1024 * 1024

  /** KRaft mode configs */
  val EmptyNodeId: Int = -1

  /************* Authorizer Configuration ***********/
  val AuthorizerClassName = ""

  /** ********* Socket Server Configuration ***********/
  val Listeners = "PLAINTEXT://:9092"
  val ListenerSecurityProtocolMap: String = EndPoint.DefaultSecurityProtocolMap.map { case (listenerName, securityProtocol) =>
    s"${listenerName.value}:${securityProtocol.name}"
  }.mkString(",")

  val SocketSendBufferBytes: Int = 100 * 1024
  val SocketReceiveBufferBytes: Int = 100 * 1024
  val SocketRequestMaxBytes: Int = 100 * 1024 * 1024
  val SocketListenBacklogSize: Int = 50
  val MaxConnectionsPerIp: Int = Int.MaxValue
  val MaxConnectionsPerIpOverrides: String = ""
  val MaxConnections: Int = Int.MaxValue
  val MaxConnectionCreationRate: Int = Int.MaxValue
  val ConnectionsMaxIdleMs = 10 * 60 * 1000L
  val RequestTimeoutMs = 30000
  val ConnectionSetupTimeoutMs = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS
  val ConnectionSetupTimeoutMaxMs = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS
  val FailedAuthenticationDelayMs = 100

  /** ********* Log Configuration ***********/
  val NumPartitions = 1
  val LogDir = "/tmp/kafka-logs"
  val LogSegmentBytes = 1 * 1024 * 1024 * 1024
  val LogRollHours = 24 * 7
  val LogRollJitterHours = 0
  val LogRetentionHours = 24 * 7

  val LogRetentionBytes = -1L
  val LogCleanupIntervalMs = 5 * 60 * 1000L
  val Delete = "delete"
  val Compact = "compact"
  val LogCleanupPolicy = Delete
  val LogCleanerThreads = 1
  val LogCleanerIoMaxBytesPerSecond = Double.MaxValue
  val LogCleanerDedupeBufferSize = 128 * 1024 * 1024L
  val LogCleanerIoBufferSize = 512 * 1024
  val LogCleanerDedupeBufferLoadFactor = 0.9d
  val LogCleanerBackoffMs = 15 * 1000
  val LogCleanerMinCleanRatio = 0.5d
  val LogCleanerEnable = true
  val LogCleanerDeleteRetentionMs = 24 * 60 * 60 * 1000L
  val LogCleanerMinCompactionLagMs = 0L
  val LogCleanerMaxCompactionLagMs = Long.MaxValue
  val LogIndexSizeMaxBytes = 10 * 1024 * 1024
  val LogIndexIntervalBytes = 4096
  val LogFlushIntervalMessages = Long.MaxValue
  val LogDeleteDelayMs = 60000
  val LogFlushSchedulerIntervalMs = Long.MaxValue
  val LogFlushOffsetCheckpointIntervalMs = 60000
  val LogFlushStartOffsetCheckpointIntervalMs = 60000
  val LogPreAllocateEnable = false

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  val LogMessageFormatVersion = IBP_3_0_IV1.version

  val LogMessageTimestampType = "CreateTime"
  val LogMessageTimestampDifferenceMaxMs = Long.MaxValue
  val NumRecoveryThreadsPerDataDir = 1
  val AutoCreateTopicsEnable = true
  val MinInSyncReplicas = 1
  val MessageDownConversionEnable = true

  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMs = RequestTimeoutMs
  val ControllerMessageQueueSize = Int.MaxValue
  val DefaultReplicationFactor = 1
  val ReplicaLagTimeMaxMs = 30000L
  val ReplicaSocketTimeoutMs = 30 * 1000
  val ReplicaSocketReceiveBufferBytes = 64 * 1024
  val ReplicaFetchMaxBytes = 1024 * 1024
  val ReplicaFetchWaitMaxMs = 500
  val ReplicaFetchMinBytes = 1
  val ReplicaFetchResponseMaxBytes = 10 * 1024 * 1024
  val NumReplicaFetchers = 1
  val ReplicaFetchBackoffMs = 1000
  val ReplicaHighWatermarkCheckpointIntervalMs = 5000L
  val FetchPurgatoryPurgeIntervalRequests = 1000
  val ProducerPurgatoryPurgeIntervalRequests = 1000
  val DeleteRecordsPurgatoryPurgeIntervalRequests = 1
  val AutoLeaderRebalanceEnable = true
  val LeaderImbalancePerBrokerPercentage = 10
  val LeaderImbalanceCheckIntervalSeconds = 300
  val UncleanLeaderElectionEnable = false
  val InterBrokerSecurityProtocol = SecurityProtocol.PLAINTEXT.toString
  val InterBrokerProtocolVersion = MetadataVersion.latest.version

  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetries = 3
  val ControlledShutdownRetryBackoffMs = 5000
  val ControlledShutdownEnable = true

  /** ********* Group coordinator configuration ***********/
  val GroupMinSessionTimeoutMs = 6000
  val GroupMaxSessionTimeoutMs = 1800000
  val GroupInitialRebalanceDelayMs = 3000
  val GroupMaxSize: Int = Int.MaxValue

  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSize = OffsetConfig.DefaultMaxMetadataSize
  val OffsetsLoadBufferSize = OffsetConfig.DefaultLoadBufferSize
  val OffsetsTopicReplicationFactor = OffsetConfig.DefaultOffsetsTopicReplicationFactor
  val OffsetsTopicPartitions: Int = OffsetConfig.DefaultOffsetsTopicNumPartitions
  val OffsetsTopicSegmentBytes: Int = OffsetConfig.DefaultOffsetsTopicSegmentBytes
  val OffsetsTopicCompressionCodec: Int = OffsetConfig.DefaultOffsetsTopicCompressionCodec.codec
  val OffsetsRetentionMinutes: Int = 7 * 24 * 60
  val OffsetsRetentionCheckIntervalMs: Long = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs
  val OffsetCommitTimeoutMs = OffsetConfig.DefaultOffsetCommitTimeoutMs
  val OffsetCommitRequiredAcks = OffsetConfig.DefaultOffsetCommitRequiredAcks

  /** ********* Transaction management configuration ***********/
  val TransactionalIdExpirationMs = TransactionStateManager.DefaultTransactionalIdExpirationMs
  val TransactionsMaxTimeoutMs = TransactionStateManager.DefaultTransactionsMaxTimeoutMs
  val TransactionsTopicMinISR = TransactionLog.DefaultMinInSyncReplicas
  val TransactionsLoadBufferSize = TransactionLog.DefaultLoadBufferSize
  val TransactionsTopicReplicationFactor = TransactionLog.DefaultReplicationFactor
  val TransactionsTopicPartitions = TransactionLog.DefaultNumPartitions
  val TransactionsTopicSegmentBytes = TransactionLog.DefaultSegmentBytes
  val TransactionsAbortTimedOutTransactionsCleanupIntervalMS = TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs
  val TransactionsRemoveExpiredTransactionsCleanupIntervalMS = TransactionStateManager.DefaultRemoveExpiredTransactionalIdsIntervalMs

  val ProducerIdExpirationMs = 86400000
  val ProducerIdExpirationCheckIntervalMs = 600000

  /** ********* Fetch Configuration **************/
  val MaxIncrementalFetchSessionCacheSlots = 1000
  val FetchMaxBytes = 55 * 1024 * 1024

  /** ********* Quota Configuration ***********/
  val NumQuotaSamples: Int = ClientQuotaManagerConfig.DefaultNumQuotaSamples
  val QuotaWindowSizeSeconds: Int = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
  val NumReplicationQuotaSamples: Int = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples
  val ReplicationQuotaWindowSizeSeconds: Int = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
  val NumAlterLogDirsReplicationQuotaSamples: Int = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples
  val AlterLogDirsReplicationQuotaWindowSizeSeconds: Int = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
  val NumControllerQuotaSamples: Int = ClientQuotaManagerConfig.DefaultNumQuotaSamples
  val ControllerQuotaWindowSizeSeconds: Int = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds

  /** ********* Transaction Configuration ***********/
  val TransactionalIdExpirationMsDefault = 604800000

  val DeleteTopicEnable = true

  val CompressionType = ProducerCompressionCodec.name

  val MaxIdMapSnapshots = 2
  /** ********* Kafka Metrics Configuration ***********/
  val MetricNumSamples = 2
  val MetricSampleWindowMs = 30000
  val MetricReporterClasses = ""
  val MetricRecordingLevel = Sensor.RecordingLevel.INFO.toString()
  val AutoIncludeJmxReporter = true


  /** ********* Kafka Yammer Metrics Reporter Configuration ***********/
  val KafkaMetricReporterClasses = ""
  val KafkaMetricsPollingIntervalSeconds = 10

  /** ********* SSL configuration ***********/
  val SslProtocol = SslConfigs.DEFAULT_SSL_PROTOCOL
  val SslEnabledProtocols = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS
  val SslKeystoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE
  val SslTruststoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE
  val SslKeyManagerAlgorithm = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM
  val SslTrustManagerAlgorithm = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM
  val SslEndpointIdentificationAlgorithm = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM
  val SslClientAuthentication = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT)
  val SslClientAuthenticationValidValues = SslClientAuth.VALUES.asScala.map(v => v.toString().toLowerCase(Locale.ROOT)).asJava.toArray(new Array[String](0))
  val SslPrincipalMappingRules = BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES

    /** ********* General Security configuration ***********/
  val ConnectionsMaxReauthMsDefault = 0L
  val DefaultServerMaxMaxReceiveSize = BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE
  val DefaultPrincipalBuilder = classOf[DefaultKafkaPrincipalBuilder]

  /** ********* Sasl configuration ***********/
  val SaslMechanismInterBrokerProtocol = SaslConfigs.DEFAULT_SASL_MECHANISM
  val SaslEnabledMechanisms = BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS
  val SaslKerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRules = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES
  val SaslLoginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS
  val SaslLoginRetryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS
  val SaslLoginRetryBackoffMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS
  val SaslOAuthBearerScopeClaimName = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME
  val SaslOAuthBearerSubClaimName = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME
  val SaslOAuthBearerJwksEndpointRefreshMs = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS
  val SaslOAuthBearerJwksEndpointRetryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS
  val SaslOAuthBearerJwksEndpointRetryBackoffMs = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS
  val SaslOAuthBearerClockSkewSeconds = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS

  /** ********* Delegation Token configuration ***********/
  val DelegationTokenMaxLifeTimeMsDefault = 7 * 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryTimeMsDefault = 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryCheckIntervalMsDefault = 1 * 60 * 60 * 1000L

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderCipherAlgorithm = "AES/CBC/PKCS5Padding"
  val PasswordEncoderKeyLength = 128
  val PasswordEncoderIterations = 4096

  /** ********* Raft Quorum Configuration *********/
  val QuorumVoters = RaftConfig.DEFAULT_QUORUM_VOTERS
  val QuorumElectionTimeoutMs = RaftConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS
  val QuorumFetchTimeoutMs = RaftConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS
  val QuorumElectionBackoffMs = RaftConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS
  val QuorumLingerMs = RaftConfig.DEFAULT_QUORUM_LINGER_MS
  val QuorumRequestTimeoutMs = RaftConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS
  val QuorumRetryBackoffMs = RaftConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS
}

object KafkaConfig {

  private val LogConfigPrefix = "log."

  def main(args: Array[String]): Unit = {
    System.out.println(configDef.toHtml(4, (config: String) => "brokerconfigs_" + config,
      DynamicBrokerConfig.dynamicConfigUpdateModes))
  }

  /** ********* Zookeeper Configuration ***********/

  /**
   * zookeeper.connect
   * 用于指定 Kafka 集群中的 ZooKeeper 的连接字符串。
   */
  val ZkConnectProp = "zookeeper.connect"
  /**
   * zookeeper.session.timeout.ms
   * 用于指定与 ZooKeeper 会话的超时时间。如果超过这格式件没有收到心跳则超时
   */
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms"
  /**
   * zookeeper.connection.timeout.ms
   * 用于指定客户端与 ZooKeeper 服务器建立连接时的超时时间（以毫秒为单位）。
   * */
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms"
  /**
   * zookeeper.connect 用于指定 Kafka 集群中的 ZooKeeper 的连接字符串。
   * Apache Kafka 中与 ZooKeeper 访问控制列表（ACL）相关的一个配置项。
   * ACL 是 ZooKeeper 中用于控制访问权限的机制，通过设置 ACL 可以控制哪个用户或客户端对某个节点（znode）具有读取、写入、创建、删除等权限。
   */
  val ZkEnableSecureAclsProp = "zookeeper.set.acl"
  /**
   * zookeeper.max.in.flight.requests
   * 用于限制客户端与 ZooKeeper 服务器之间在同一时间内未完成的请求数目。这有助于防止客户端发送过多的并发请求，从而可能导致 ZooKeeper 服务器过载。
   */
  val ZkMaxInFlightRequestsProp = "zookeeper.max.in.flight.requests"
  /**
   * zookeeper.ssl.client.enable
   * 用于启用客户端与 ZooKeeper 之间的 SSL/TLS 加密连接。
   * 以下为相关配置：
   * zookeeper.ssl.keystore.location：密钥库文件的位置。
   * zookeeper.ssl.keystore.password：密钥库的密码。
   * zookeeper.ssl.key.password：密钥的密码。
   * zookeeper.ssl.truststore.location：信任库文件的位置。
   * zookeeper.ssl.truststore.password：信任库的密码。
   */
  val ZkSslClientEnableProp = "zookeeper.ssl.client.enable"
  /**
   * zookeeper.clientCnxnSocket
   * 用于指定客户端连接套接字的实现类。这个配置项允许用户自定义或选择不同的客户端连接方式，以满足特定的网络需求或性能要求。
   */
  val ZkClientCnxnSocketProp = "zookeeper.clientCnxnSocket"
  /**
   * zookeeper.ssl.keystore.location
   * 用于指定keystore地址
   * 无默认值
   */
  val ZkSslKeyStoreLocationProp = "zookeeper.ssl.keystore.location"
  /**
   * zookeeper.ssl.keystore.password
   * 用于指定keystore的密码
   * 无默认值
   */
  val ZkSslKeyStorePasswordProp = "zookeeper.ssl.keystore.password"
  /**
   * zookeeper.ssl.keystore.type
   * 用于指定密钥库的类型。常见的密钥库类型包括 JKS（Java KeyStore）和 PKCS12。
   * 该参数的默认值是 JKS。
   */
  val ZkSslKeyStoreTypeProp = "zookeeper.ssl.keystore.type"
  /**
   * zookeeper.ssl.truststore.location
   * 用于指定truststore地址
   * 无默认值
   */
  val ZkSslTrustStoreLocationProp = "zookeeper.ssl.truststore.location"
  /**
   * zookeeper.ssl.truststore.password
   * 用于指定truststore密码
   * 无默认值
   */
  val ZkSslTrustStorePasswordProp = "zookeeper.ssl.truststore.password"
  /**
   * zookeeper.ssl.truststore.password
   * 用于指定密钥库的类型。常见的密钥库类型包括 JKS（Java KeyStore）和 PKCS12。
   * 该参数的默认值是 JKS。
   */
  val ZkSslTrustStoreTypeProp = "zookeeper.ssl.truststore.type"
  /**
   * zookeeper.ssl.protocol
   * 用于指定客户端与 ZooKeeper 之间使用的 SSL/TLS 协议版本。
   * 该参数的默认值是 TLSv1.2。
   */
  val ZkSslProtocolProp = "zookeeper.ssl.protocol"
  /**
   * zookeeper.ssl.protocol
   * 指定 Kafka 客户端与 ZooKeeper 之间启用的 SSL/TLS 协议版本列表。用于设置允许使用的多个协议版本。
   * 该参数的默认值是 TLSv1.2。
   */
  val ZkSslEnabledProtocolsProp = "zookeeper.ssl.enabled.protocols"
  /**
   * zookeeper.ssl.cipher.suites
   * 用于指定客户端与 ZooKeeper 之间启用的 SSL/TLS 密码套件列表。
   * 密码套件定义了加密算法、密钥交换算法和消息认证码（MAC）算法的组合，用于保护通信数据的机密性和完整性。
   * 无默认值
   */
  val ZkSslCipherSuitesProp = "zookeeper.ssl.cipher.suites"
  /**
   * zookeeper.ssl.endpoint.identification.algorithm
   * 端点识别通常用于验证服务器的身份，以防止中间人攻击。
   * 用于指定在 SSL/TLS 连接中进行端点识别时使用的算法。
   * 该参数的默认值是 HTTPS
   */
  val ZkSslEndpointIdentificationAlgorithmProp = "zookeeper.ssl.endpoint.identification.algorithm"
  /**
   * zookeeper.ssl.crl.enable
   * 用于启用或禁用 SSL/TLS 连接中的证书吊销列表（CRL）检查。
   * 证书吊销列表用于列出已被吊销的证书，以防止使用这些证书进行通信。
   * 该参数的默认值是 false
   */
  val ZkSslCrlEnableProp = "zookeeper.ssl.crl.enable"
  /**
   * zookeeper.ssl.ocsp.enable
   * 用于启用或禁用 SSL/TLS 连接中的在线证书状态协议（OCSP）检查。OCSP 用于实时验证证书的状态，以确定其是否被吊销。
   * 该参数的默认值是 false
   */
  val ZkSslOcspEnableProp = "zookeeper.ssl.ocsp.enable"

  // a map from the Kafka config to the corresponding ZooKeeper Java system property
  private[kafka] val ZkSslConfigToSystemPropertyMap: Map[String, String] = Map(
    ZkSslClientEnableProp -> ZKClientConfig.SECURE_CLIENT,
    ZkClientCnxnSocketProp -> ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET,
    ZkSslKeyStoreLocationProp -> "zookeeper.ssl.keyStore.location",
    ZkSslKeyStorePasswordProp -> "zookeeper.ssl.keyStore.password",
    ZkSslKeyStoreTypeProp -> "zookeeper.ssl.keyStore.type",
    ZkSslTrustStoreLocationProp -> "zookeeper.ssl.trustStore.location",
    ZkSslTrustStorePasswordProp -> "zookeeper.ssl.trustStore.password",
    ZkSslTrustStoreTypeProp -> "zookeeper.ssl.trustStore.type",
    ZkSslProtocolProp -> "zookeeper.ssl.protocol",
    ZkSslEnabledProtocolsProp -> "zookeeper.ssl.enabledProtocols",
    ZkSslCipherSuitesProp -> "zookeeper.ssl.ciphersuites",
    ZkSslEndpointIdentificationAlgorithmProp -> "zookeeper.ssl.hostnameVerification",
    ZkSslCrlEnableProp -> "zookeeper.ssl.crl",
    ZkSslOcspEnableProp -> "zookeeper.ssl.ocsp")

  private[kafka] def zooKeeperClientProperty(clientConfig: ZKClientConfig, kafkaPropName: String): Option[String] = {
    Option(clientConfig.getProperty(ZkSslConfigToSystemPropertyMap(kafkaPropName)))
  }

  private[kafka] def setZooKeeperClientProperty(clientConfig: ZKClientConfig, kafkaPropName: String, kafkaPropValue: Any): Unit = {
    clientConfig.setProperty(ZkSslConfigToSystemPropertyMap(kafkaPropName),
      kafkaPropName match {
        case ZkSslEndpointIdentificationAlgorithmProp => (kafkaPropValue.toString.toUpperCase == "HTTPS").toString
        case ZkSslEnabledProtocolsProp | ZkSslCipherSuitesProp => kafkaPropValue match {
          case list: java.util.List[_] => list.asScala.mkString(",")
          case _ => kafkaPropValue.toString
        }
        case _ => kafkaPropValue.toString
    })
  }

  // For ZooKeeper TLS client authentication to be enabled the client must (at a minimum) configure itself as using TLS
  // with both a client connection socket and a key store location explicitly set.
  private[kafka] def zkTlsClientAuthEnabled(zkClientConfig: ZKClientConfig): Boolean = {
    zooKeeperClientProperty(zkClientConfig, ZkSslClientEnableProp).contains("true") &&
      zooKeeperClientProperty(zkClientConfig, ZkClientCnxnSocketProp).isDefined &&
      zooKeeperClientProperty(zkClientConfig, ZkSslKeyStoreLocationProp).isDefined
  }


  /**
   * broker.id.generation.enable
   * 用于控制是否启用自动生成 broker.id 的功能。
   * 默然值 false
   */
  val BrokerIdGenerationEnableProp = "broker.id.generation.enable"
  /**
   * reserved.broker.max.id
   * 用于指定保留的 broker ID 的最大值。这个参数用于防止自动生成的 broker ID 与用户手动分配的 broker ID 冲突。
   * 默然值 1000
   */
  val MaxReservedBrokerIdProp = "reserved.broker.max.id"
  /**
   * <p>broker.id</p>
   * <p>用于唯一标识每个 Kafka broker（服务器）在集群中的身份。每个 broker 必须有一个唯一的 broker.id，以便 Kafka 能够正确地管理和协调集群中的所有 broker。</p>
   * <p>默认值是 -1，表示自动生成 broker.id。</p>
   */
  val BrokerIdProp = "broker.id"
  val MessageMaxBytesProp = "message.max.bytes"
  val NumNetworkThreadsProp = "num.network.threads"
  val NumIoThreadsProp = "num.io.threads"
  /**
   * <p>background.threads</p>
   * <p>配置项用于设置 Kafka 代理中用于执行后台任务的线程数量。这些后台任务包括日志清理、文件关闭以及其他维护工作。</p>
   * <p>默认值 10</p>
   */
  val BackgroundThreadsProp = "background.threads"
  val NumReplicaAlterLogDirsThreadsProp = "num.replica.alter.log.dirs.threads"
  /**
   * <p>broker.id</p>
   * <p>用于控制在处理请求之前，I/O 线程允许的最大请求队列数量。这个参数对管理 Kafka broker 的内存使用和性能有重要影响。</p>
   * <p>默认值 500</p>
   */
  val QueuedMaxRequestsProp = "queued.max.requests"

  /**
   * <p>broker.id</p>
   * <p>用于限制在内存中排队的请求总字节数。当达到此限制时，不会读取更多的请求。这有助于防止由于请求量过大而导致的内存溢出（Out of Memory，OOM）问题。</p>
   * <p>默认值 -1</p>
   */
  val QueuedMaxBytesProp = "queued.max.request.bytes"

  val RequestTimeoutMsProp = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG

  val ConnectionSetupTimeoutMsProp = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG

  val ConnectionSetupTimeoutMaxMsProp = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG

  /** KRaft mode configs */
  val ProcessRolesProp = "process.roles"
  /**
   * <h1>initial.broker.registration.timeout.ms</h1>
   * <p>用于指定在初次注册到控制器仲裁（controller quorum）时，等待声明失败并退出 broker 进程的最大时间（以毫秒为单位）。</p>
   * <p>这个参数在确保 broker 能够及时注册并参与集群协调时起到了关键作用。</p>
   * <p>默认值是 60000 </p>
   */
  val InitialBrokerRegistrationTimeoutMsProp = "initial.broker.registration.timeout.ms"
  /**
   * <h1>broker.heartbeat.interval.ms</h1>
   * <p>用于指定 Kafka broker 向集群控制器（controller）发送心跳的间隔时间（以毫秒为单位）。</p>
   * <p>这个参数帮助控制器跟踪每个 broker 的健康状态，确保集群的稳定运行。</p>
   * <p>默认值是 2000 </p>
   */
  val BrokerHeartbeatIntervalMsProp = "broker.heartbeat.interval.ms"
  /**
   * <h1>broker.heartbeat.interval.ms</h1>
   * <p>用于指定在 Kafka 使用 KRaft 模式时，如果控制器未收到 broker 的心跳请求，认为该 broker 不再存活的时间间隔（以毫秒为单位）。</p>
   * <p>默认值是 2000 </p>
   */
  val BrokerSessionTimeoutMsProp = "broker.session.timeout.ms"
  /**
   * <h1>broker.id</h1>
   * <p>在 Kafka 新引入的 Raft 协议中，node.id 用于标识 Raft 集群中的每个节点。</p>
   * <p>默认值是 -1，表示自动生成 broker.id。</p>
   */
  val NodeIdProp = "node.id"
  val MetadataLogDirProp = "metadata.log.dir"
  val MetadataSnapshotMaxNewRecordBytesProp = "metadata.log.max.record.bytes.between.snapshots"
  val MetadataSnapshotMaxIntervalMsProp = "metadata.log.max.snapshot.interval.ms"
  val ControllerListenerNamesProp = "controller.listener.names"
  val SaslMechanismControllerProtocolProp = "sasl.mechanism.controller.protocol"
  val MetadataLogSegmentMinBytesProp = "metadata.log.segment.min.bytes"
  val MetadataLogSegmentBytesProp = "metadata.log.segment.bytes"
  val MetadataLogSegmentMillisProp = "metadata.log.segment.ms"
  val MetadataMaxRetentionBytesProp = "metadata.max.retention.bytes"
  val MetadataMaxRetentionMillisProp = "metadata.max.retention.ms"
  val QuorumVotersProp = RaftConfig.QUORUM_VOTERS_CONFIG
  val MetadataMaxIdleIntervalMsProp = "metadata.max.idle.interval.ms"

  /** ZK to KRaft Migration configs */

  /**
   * <h1>zookeeper.metadata.migration.enable</h1>
   * <p>用于在将 Kafka 集群从基于 ZooKeeper 的元数据管理迁移到 KRaft（Kafka Raft）模式时启用元数据迁移。</p>
   * <p>启用此配置后，Kafka 将开始将 ZooKeeper 中存储的元数据迁移到 KRaft 控制器中管理。</p>
   * <p>默认值 false</p>
   */
  val MigrationEnabledProp = "zookeeper.metadata.migration.enable"

  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameProp = "authorizer.class.name"
  val EarlyStartListenersProp = "early.start.listeners"

  /** ********* Socket Server Configuration ***********/

  /**
   * listeners socket用于监听网络请求的地址
   * advertised.listeners 对外发布的一个地址（只是为了让其他机器或者发送段知道我应该往哪里发数据）
   */
  val ListenersProp = "listeners"
  val AdvertisedListenersProp = "advertised.listeners"
  /**
   * listener.security.protocol.map 是 Kafka 中的一项配置属性，用于映射监听器名称到安全协议。这项配置允许 Kafka 为不同的监听器指定不同的安全协议，
   * 例如 PLAINTEXT、SSL、SASL_PLAINTEXT 或 SASL_SSL。
   * listeners=PLAINTEXT://:9092,SSL://:9093
   * listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL
   */
  val ListenerSecurityProtocolMapProp = "listener.security.protocol.map"
  val ControlPlaneListenerNameProp = "control.plane.listener.name"
  /**
   * <h1>socket.send.buffer.bytes</h1>
   * <p>用于在将 Kafka 集群从基于 ZooKeeper 的元数据管理迁移到 KRaft（Kafka Raft）模式时启用元数据迁移。</p>
   * <p>启用此配置后，Kafka 将开始将 ZooKeeper 中存储的元数据迁移到 KRaft 控制器中管理。</p>
   * <p>默认值 false</p>
   */
  val SocketSendBufferBytesProp = "socket.send.buffer.bytes"
  val SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes"
  val SocketRequestMaxBytesProp = "socket.request.max.bytes"
  val SocketListenBacklogSizeProp = "socket.listen.backlog.size"
  val MaxConnectionsPerIpProp = "max.connections.per.ip"
  val MaxConnectionsPerIpOverridesProp = "max.connections.per.ip.overrides"
  val MaxConnectionsProp = "max.connections"
  val MaxConnectionCreationRateProp = "max.connection.creation.rate"
  val ConnectionsMaxIdleMsProp = "connections.max.idle.ms"
  val FailedAuthenticationDelayMsProp = "connection.failed.authentication.delay.ms"
  /***************** rack configuration *************/
  val RackProp = "broker.rack"
  /** ********* Log Configuration ***********/
  val NumPartitionsProp = "num.partitions"
  val LogDirsProp = LogConfigPrefix + "dirs"
  val LogDirProp = LogConfigPrefix + "dir"
  val LogSegmentBytesProp = LogConfigPrefix + "segment.bytes"

  val LogRollTimeMillisProp = LogConfigPrefix + "roll.ms"
  val LogRollTimeHoursProp = LogConfigPrefix + "roll.hours"

  val LogRollTimeJitterMillisProp = LogConfigPrefix + "roll.jitter.ms"
  val LogRollTimeJitterHoursProp = LogConfigPrefix + "roll.jitter.hours"

  val LogRetentionTimeMillisProp = LogConfigPrefix + "retention.ms"
  val LogRetentionTimeMinutesProp = LogConfigPrefix + "retention.minutes"
  val LogRetentionTimeHoursProp = LogConfigPrefix + "retention.hours"

  val LogRetentionBytesProp = LogConfigPrefix + "retention.bytes"
  val LogCleanupIntervalMsProp = LogConfigPrefix + "retention.check.interval.ms"
  val LogCleanupPolicyProp = LogConfigPrefix + "cleanup.policy"
  val LogCleanerThreadsProp = LogConfigPrefix + "cleaner.threads"
  val LogCleanerIoMaxBytesPerSecondProp = LogConfigPrefix + "cleaner.io.max.bytes.per.second"
  val LogCleanerDedupeBufferSizeProp = LogConfigPrefix + "cleaner.dedupe.buffer.size"
  val LogCleanerIoBufferSizeProp = LogConfigPrefix + "cleaner.io.buffer.size"
  val LogCleanerDedupeBufferLoadFactorProp = LogConfigPrefix + "cleaner.io.buffer.load.factor"
  val LogCleanerBackoffMsProp = LogConfigPrefix + "cleaner.backoff.ms"
  val LogCleanerMinCleanRatioProp = LogConfigPrefix + "cleaner.min.cleanable.ratio"
  val LogCleanerEnableProp = LogConfigPrefix + "cleaner.enable"
  val LogCleanerDeleteRetentionMsProp = LogConfigPrefix + "cleaner.delete.retention.ms"
  val LogCleanerMinCompactionLagMsProp = LogConfigPrefix + "cleaner.min.compaction.lag.ms"
  val LogCleanerMaxCompactionLagMsProp = LogConfigPrefix + "cleaner.max.compaction.lag.ms"
  val LogIndexSizeMaxBytesProp = LogConfigPrefix + "index.size.max.bytes"
  val LogIndexIntervalBytesProp = LogConfigPrefix + "index.interval.bytes"
  val LogFlushIntervalMessagesProp = LogConfigPrefix + "flush.interval.messages"
  val LogDeleteDelayMsProp = LogConfigPrefix + "segment.delete.delay.ms"
  val LogFlushSchedulerIntervalMsProp = LogConfigPrefix + "flush.scheduler.interval.ms"
  val LogFlushIntervalMsProp = LogConfigPrefix + "flush.interval.ms"
  val LogFlushOffsetCheckpointIntervalMsProp = LogConfigPrefix + "flush.offset.checkpoint.interval.ms"
  val LogFlushStartOffsetCheckpointIntervalMsProp = LogConfigPrefix + "flush.start.offset.checkpoint.interval.ms"
  val LogPreAllocateProp = LogConfigPrefix + "preallocate"

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  val LogMessageFormatVersionProp = LogConfigPrefix + "message.format.version"

  val LogMessageTimestampTypeProp = LogConfigPrefix + "message.timestamp.type"
  val LogMessageTimestampDifferenceMaxMsProp = LogConfigPrefix + "message.timestamp.difference.max.ms"
  val LogMaxIdMapSnapshotsProp = LogConfigPrefix + "max.id.map.snapshots"
  /**
   * <h1>num.recovery.threads.per.data.dir</h1>
   * <p>用于控制每个数据目录日志恢复线程数量</p>
   * <p>默认值是 10</p>
   */
  val NumRecoveryThreadsPerDataDirProp = "num.recovery.threads.per.data.dir"
  val AutoCreateTopicsEnableProp = "auto.create.topics.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  val CreateTopicPolicyClassNameProp = "create.topic.policy.class.name"
  val AlterConfigPolicyClassNameProp = "alter.config.policy.class.name"
  val LogMessageDownConversionEnableProp = LogConfigPrefix + "message.downconversion.enable"
  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsProp = "controller.socket.timeout.ms"
  val DefaultReplicationFactorProp = "default.replication.factor"
  val ReplicaLagTimeMaxMsProp = "replica.lag.time.max.ms"
  val ReplicaSocketTimeoutMsProp = "replica.socket.timeout.ms"
  val ReplicaSocketReceiveBufferBytesProp = "replica.socket.receive.buffer.bytes"
  val ReplicaFetchMaxBytesProp = "replica.fetch.max.bytes"
  val ReplicaFetchWaitMaxMsProp = "replica.fetch.wait.max.ms"
  val ReplicaFetchMinBytesProp = "replica.fetch.min.bytes"
  val ReplicaFetchResponseMaxBytesProp = "replica.fetch.response.max.bytes"
  val ReplicaFetchBackoffMsProp = "replica.fetch.backoff.ms"
  val NumReplicaFetchersProp = "num.replica.fetchers"
  val ReplicaHighWatermarkCheckpointIntervalMsProp = "replica.high.watermark.checkpoint.interval.ms"
  val FetchPurgatoryPurgeIntervalRequestsProp = "fetch.purgatory.purge.interval.requests"
  val ProducerPurgatoryPurgeIntervalRequestsProp = "producer.purgatory.purge.interval.requests"
  val DeleteRecordsPurgatoryPurgeIntervalRequestsProp = "delete.records.purgatory.purge.interval.requests"
  val AutoLeaderRebalanceEnableProp = "auto.leader.rebalance.enable"
  val LeaderImbalancePerBrokerPercentageProp = "leader.imbalance.per.broker.percentage"
  val LeaderImbalanceCheckIntervalSecondsProp = "leader.imbalance.check.interval.seconds"
  val UncleanLeaderElectionEnableProp = "unclean.leader.election.enable"
  val InterBrokerSecurityProtocolProp = "security.inter.broker.protocol"
  val InterBrokerProtocolVersionProp = "inter.broker.protocol.version"
  val InterBrokerListenerNameProp = "inter.broker.listener.name"
  val ReplicaSelectorClassProp = "replica.selector.class"
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesProp = "controlled.shutdown.max.retries"
  val ControlledShutdownRetryBackoffMsProp = "controlled.shutdown.retry.backoff.ms"
  val ControlledShutdownEnableProp = "controlled.shutdown.enable"
  /** ********* Group coordinator configuration ***********/
  val GroupMinSessionTimeoutMsProp = "group.min.session.timeout.ms"
  val GroupMaxSessionTimeoutMsProp = "group.max.session.timeout.ms"
  val GroupInitialRebalanceDelayMsProp = "group.initial.rebalance.delay.ms"
  val GroupMaxSizeProp = "group.max.size"
  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSizeProp = "offset.metadata.max.bytes"
  val OffsetsLoadBufferSizeProp = "offsets.load.buffer.size"
  val OffsetsTopicReplicationFactorProp = "offsets.topic.replication.factor"
  val OffsetsTopicPartitionsProp = "offsets.topic.num.partitions"
  val OffsetsTopicSegmentBytesProp = "offsets.topic.segment.bytes"
  val OffsetsTopicCompressionCodecProp = "offsets.topic.compression.codec"
  val OffsetsRetentionMinutesProp = "offsets.retention.minutes"
  val OffsetsRetentionCheckIntervalMsProp = "offsets.retention.check.interval.ms"
  val OffsetCommitTimeoutMsProp = "offsets.commit.timeout.ms"
  val OffsetCommitRequiredAcksProp = "offsets.commit.required.acks"
  /** ********* Transaction management configuration ***********/
  val TransactionalIdExpirationMsProp = "transactional.id.expiration.ms"
  val TransactionsMaxTimeoutMsProp = "transaction.max.timeout.ms"
  val TransactionsTopicMinISRProp = "transaction.state.log.min.isr"
  val TransactionsLoadBufferSizeProp = "transaction.state.log.load.buffer.size"
  val TransactionsTopicPartitionsProp = "transaction.state.log.num.partitions"
  val TransactionsTopicSegmentBytesProp = "transaction.state.log.segment.bytes"
  val TransactionsTopicReplicationFactorProp = "transaction.state.log.replication.factor"
  val TransactionsAbortTimedOutTransactionCleanupIntervalMsProp = "transaction.abort.timed.out.transaction.cleanup.interval.ms"
  val TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp = "transaction.remove.expired.transaction.cleanup.interval.ms"

  val ProducerIdExpirationMsProp = "producer.id.expiration.ms"
  val ProducerIdExpirationCheckIntervalMsProp = "producer.id.expiration.check.interval.ms"

  /** ********* Fetch Configuration **************/
  val MaxIncrementalFetchSessionCacheSlots = "max.incremental.fetch.session.cache.slots"
  val FetchMaxBytes = "fetch.max.bytes"

  /** ********* Quota Configuration ***********/
  val NumQuotaSamplesProp = "quota.window.num"
  val NumReplicationQuotaSamplesProp = "replication.quota.window.num"
  val NumAlterLogDirsReplicationQuotaSamplesProp = "alter.log.dirs.replication.quota.window.num"
  val NumControllerQuotaSamplesProp = "controller.quota.window.num"
  val QuotaWindowSizeSecondsProp = "quota.window.size.seconds"
  val ReplicationQuotaWindowSizeSecondsProp = "replication.quota.window.size.seconds"
  val AlterLogDirsReplicationQuotaWindowSizeSecondsProp = "alter.log.dirs.replication.quota.window.size.seconds"
  val ControllerQuotaWindowSizeSecondsProp = "controller.quota.window.size.seconds"
  val ClientQuotaCallbackClassProp = "client.quota.callback.class"

  val DeleteTopicEnableProp = "delete.topic.enable"
  val CompressionTypeProp = "compression.type"

  /** ********* Kafka Metrics Configuration ***********/
  val MetricSampleWindowMsProp = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG
  val MetricNumSamplesProp: String = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG
  val MetricReporterClassesProp: String = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG
  val MetricRecordingLevelProp: String = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG
  @deprecated
  val AutoIncludeJmxReporterProp: String = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG

  /** ********* Kafka Yammer Metrics Reporters Configuration ***********/

  /**
   * <h1>kafka.metrics.reporters</h1>
   * <p>用于指定一组类，这些类用于收集和报告 Kafka 集群的运行指标。</p>
   * <p>通过配置这个参数，Kafka 可以将各种性能和健康指标发送到不同的监控系统，如 JMX、Prometheus 或其他自定义报告系统。</p>
   * <p>默认值是 null</p>
   */
  val KafkaMetricsReporterClassesProp = "kafka.metrics.reporters"
  /**
   * <h1>kafka.metrics.reporters</h1>
   * <p>用于指定 Kafka 指标（metrics）报告器的轮询间隔时间（以秒为单位）。</p>
   * <p>此参数控制指标收集和报告的频率。</p>
   * <p>默认值是 10</p>
   */
  val KafkaMetricsPollingIntervalSecondsProp = "kafka.metrics.polling.interval.secs"

  /** ******** Common Security Configuration *************/
  val PrincipalBuilderClassProp = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG
  val ConnectionsMaxReauthMsProp = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS
  val SaslServerMaxReceiveSizeProp = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG
  val securityProviderClassProp = SecurityConfig.SECURITY_PROVIDERS_CONFIG

  /** ********* SSL Configuration ****************/
  val SslProtocolProp = SslConfigs.SSL_PROTOCOL_CONFIG
  val SslProviderProp = SslConfigs.SSL_PROVIDER_CONFIG
  val SslCipherSuitesProp = SslConfigs.SSL_CIPHER_SUITES_CONFIG
  val SslEnabledProtocolsProp = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG
  val SslKeystoreTypeProp = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG
  val SslKeystoreLocationProp = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG
  val SslKeystorePasswordProp = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
  val SslKeyPasswordProp = SslConfigs.SSL_KEY_PASSWORD_CONFIG
  val SslKeystoreKeyProp = SslConfigs.SSL_KEYSTORE_KEY_CONFIG
  val SslKeystoreCertificateChainProp = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG
  val SslTruststoreTypeProp = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG
  val SslTruststoreLocationProp = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
  val SslTruststorePasswordProp = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
  val SslTruststoreCertificatesProp = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG
  val SslKeyManagerAlgorithmProp = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG
  val SslTrustManagerAlgorithmProp = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG
  val SslEndpointIdentificationAlgorithmProp = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
  val SslSecureRandomImplementationProp = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG
  val SslClientAuthProp = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG
  val SslPrincipalMappingRulesProp = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG
  var SslEngineFactoryClassProp = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG

  /** ********* SASL Configuration ****************/
  val SaslMechanismInterBrokerProtocolProp = "sasl.mechanism.inter.broker.protocol"
  val SaslJaasConfigProp = SaslConfigs.SASL_JAAS_CONFIG
  val SaslEnabledMechanismsProp = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG
  val SaslServerCallbackHandlerClassProp = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS
  val SaslClientCallbackHandlerClassProp = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS
  val SaslLoginClassProp = SaslConfigs.SASL_LOGIN_CLASS
  val SaslLoginCallbackHandlerClassProp = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS
  val SaslKerberosServiceNameProp = SaslConfigs.SASL_KERBEROS_SERVICE_NAME
  val SaslKerberosKinitCmdProp = SaslConfigs.SASL_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactorProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitterProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeReloginProp = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRulesProp = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG
  val SaslLoginRefreshWindowFactorProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitterProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS

  val SaslLoginConnectTimeoutMsProp = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS
  val SaslLoginReadTimeoutMsProp = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS
  val SaslLoginRetryBackoffMaxMsProp = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS
  val SaslLoginRetryBackoffMsProp = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS
  val SaslOAuthBearerScopeClaimNameProp = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME
  val SaslOAuthBearerSubClaimNameProp = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME
  val SaslOAuthBearerTokenEndpointUrlProp = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
  val SaslOAuthBearerJwksEndpointUrlProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL
  val SaslOAuthBearerJwksEndpointRefreshMsProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS
  val SaslOAuthBearerJwksEndpointRetryBackoffMaxMsProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS
  val SaslOAuthBearerJwksEndpointRetryBackoffMsProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS
  val SaslOAuthBearerClockSkewSecondsProp = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS
  val SaslOAuthBearerExpectedAudienceProp = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE
  val SaslOAuthBearerExpectedIssuerProp = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER

  /** ********* Delegation Token Configuration ****************/
  val DelegationTokenSecretKeyAliasProp = "delegation.token.master.key"
  val DelegationTokenSecretKeyProp = "delegation.token.secret.key"
  val DelegationTokenMaxLifeTimeProp = "delegation.token.max.lifetime.ms"
  val DelegationTokenExpiryTimeMsProp = "delegation.token.expiry.time.ms"
  val DelegationTokenExpiryCheckIntervalMsProp = "delegation.token.expiry.check.interval.ms"

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderSecretProp = "password.encoder.secret"
  val PasswordEncoderOldSecretProp = "password.encoder.old.secret"
  val PasswordEncoderKeyFactoryAlgorithmProp = "password.encoder.keyfactory.algorithm"
  val PasswordEncoderCipherAlgorithmProp = "password.encoder.cipher.algorithm"
  val PasswordEncoderKeyLengthProp =  "password.encoder.key.length"
  val PasswordEncoderIterationsProp =  "password.encoder.iterations"

  /* Documentation */
  /** ********* Zookeeper Configuration ***********/
  val ZkConnectDoc = "Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the " +
  "host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is " +
  "down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
  "The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. " +
  "For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>."
  val ZkSessionTimeoutMsDoc = "Zookeeper session timeout"
  val ZkConnectionTimeoutMsDoc = "The max time that the client waits to establish a connection to zookeeper. If not set, the value in " + ZkSessionTimeoutMsProp + " is used"
  val ZkEnableSecureAclsDoc = "Set client to use secure ACLs"
  val ZkMaxInFlightRequestsDoc = "The maximum number of unacknowledged requests the client will send to Zookeeper before blocking."
  val ZkSslClientEnableDoc = "Set client to use TLS when connecting to ZooKeeper." +
    " An explicit value overrides any value set via the <code>zookeeper.client.secure</code> system property (note the different name)." +
    s" Defaults to false if neither is set; when true, <code>$ZkClientCnxnSocketProp</code> must be set (typically to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code>); other values to set may include " +
    ZkSslConfigToSystemPropertyMap.keys.toList.filter(x => x != ZkSslClientEnableProp && x != ZkClientCnxnSocketProp).sorted.mkString("<code>", "</code>, <code>", "</code>")
  val ZkClientCnxnSocketDoc = "Typically set to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code> when using TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the same-named <code>${ZkSslConfigToSystemPropertyMap(ZkClientCnxnSocketProp)}</code> system property."
  val ZkSslKeyStoreLocationDoc = "Keystore location when using a client-side certificate with TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslKeyStoreLocationProp)}</code> system property (note the camelCase)."
  val ZkSslKeyStorePasswordDoc = "Keystore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslKeyStorePasswordProp)}</code> system property (note the camelCase)." +
    " Note that ZooKeeper does not support a key password different from the keystore password, so be sure to set the key password in the keystore to be identical to the keystore password; otherwise the connection attempt to Zookeeper will fail."
  val ZkSslKeyStoreTypeDoc = "Keystore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslKeyStoreTypeProp)}</code> system property (note the camelCase)." +
    " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the keystore."
  val ZkSslTrustStoreLocationDoc = "Truststore location when using TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslTrustStoreLocationProp)}</code> system property (note the camelCase)."
  val ZkSslTrustStorePasswordDoc = "Truststore password when using TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslTrustStorePasswordProp)}</code> system property (note the camelCase)."
  val ZkSslTrustStoreTypeDoc = "Truststore type when using TLS connectivity to ZooKeeper." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslTrustStoreTypeProp)}</code> system property (note the camelCase)." +
    " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the truststore."
  val ZkSslProtocolDoc = "Specifies the protocol to be used in ZooKeeper TLS negotiation." +
    s" An explicit value overrides any value set via the same-named <code>${ZkSslConfigToSystemPropertyMap(ZkSslProtocolProp)}</code> system property."
  val ZkSslEnabledProtocolsDoc = "Specifies the enabled protocol(s) in ZooKeeper TLS negotiation (csv)." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslEnabledProtocolsProp)}</code> system property (note the camelCase)." +
    s" The default value of <code>null</code> means the enabled protocol will be the value of the <code>${KafkaConfig.ZkSslProtocolProp}</code> configuration property."
  val ZkSslCipherSuitesDoc = "Specifies the enabled cipher suites to be used in ZooKeeper TLS negotiation (csv)." +
    s""" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslCipherSuitesProp)}</code> system property (note the single word \"ciphersuites\").""" +
    " The default value of <code>null</code> means the list of enabled cipher suites is determined by the Java runtime being used."
  val ZkSslEndpointIdentificationAlgorithmDoc = "Specifies whether to enable hostname verification in the ZooKeeper TLS negotiation process, with (case-insensitively) \"https\" meaning ZooKeeper hostname verification is enabled and an explicit blank value meaning it is disabled (disabling it is only recommended for testing purposes)." +
    s""" An explicit value overrides any \"true\" or \"false\" value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslEndpointIdentificationAlgorithmProp)}</code> system property (note the different name and values; true implies https and false implies blank)."""
  val ZkSslCrlEnableDoc = "Specifies whether to enable Certificate Revocation List in the ZooKeeper TLS protocols." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslCrlEnableProp)}</code> system property (note the shorter name)."
  val ZkSslOcspEnableDoc = "Specifies whether to enable Online Certificate Status Protocol in the ZooKeeper TLS protocols." +
    s" Overrides any explicit value set via the <code>${ZkSslConfigToSystemPropertyMap(ZkSslOcspEnableProp)}</code> system property (note the shorter name)."
  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnableDoc = s"Enable automatic broker id generation on the server. When enabled the value configured for $MaxReservedBrokerIdProp should be reviewed."
  val MaxReservedBrokerIdDoc = "Max number that can be used for a broker.id"
  val BrokerIdDoc = "The broker id for this server. If unset, a unique broker id will be generated." +
  "To avoid conflicts between zookeeper generated broker id's and user configured broker id's, generated broker ids " +
  "start from " + MaxReservedBrokerIdProp + " + 1."
  val MessageMaxBytesDoc = TopicConfig.MAX_MESSAGE_BYTES_DOC +
    s"This can be set per topic with the topic level <code>${TopicConfig.MAX_MESSAGE_BYTES_CONFIG}</code> config."
  val NumNetworkThreadsDoc = "The number of threads that the server uses for receiving requests from the network and sending responses to the network"
  val NumIoThreadsDoc = "The number of threads that the server uses for processing requests, which may include disk I/O"
  val NumReplicaAlterLogDirsThreadsDoc = "The number of threads that can move replicas between log directories, which may include disk I/O"
  val BackgroundThreadsDoc = "The number of threads to use for various background processing tasks"
  val QueuedMaxRequestsDoc = "The number of queued requests allowed for data-plane, before blocking the network threads"
  val QueuedMaxRequestBytesDoc = "The number of queued bytes allowed before no more requests are read"
  val RequestTimeoutMsDoc = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
  val ConnectionSetupTimeoutMsDoc = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC
  val ConnectionSetupTimeoutMaxMsDoc = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC

  /** KRaft mode configs */
  val ProcessRolesDoc = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. " +
    "This configuration is only applicable for clusters in KRaft (Kafka Raft) mode (instead of ZooKeeper). Leave this config undefined or empty for Zookeeper clusters."
  val InitialBrokerRegistrationTimeoutMsDoc = "When initially registering with the controller quorum, the number of milliseconds to wait before declaring failure and exiting the broker process."
  val BrokerHeartbeatIntervalMsDoc = "The length of time in milliseconds between broker heartbeats. Used when running in KRaft mode."
  val BrokerSessionTimeoutMsDoc = "The length of time in milliseconds that a broker lease lasts if no heartbeats are made. Used when running in KRaft mode."
  val NodeIdDoc = "The node ID associated with the roles this process is playing when `process.roles` is non-empty. " +
    "This is required configuration when running in KRaft mode."
  val MetadataLogDirDoc = "This configuration determines where we put the metadata log for clusters in KRaft mode. " +
    "If it is not set, the metadata log is placed in the first log directory from log.dirs."
  val MetadataSnapshotMaxNewRecordBytesDoc = "This is the maximum number of bytes in the log between the latest " +
    "snapshot and the high-watermark needed before generating a new snapshot. The default value is " +
    s"${Defaults.MetadataSnapshotMaxNewRecordBytes}. To generate snapshots based on the time elapsed, see " +
    s"the <code>$MetadataSnapshotMaxIntervalMsProp</code> configuration. The Kafka node will generate a snapshot when " +
    "either the maximum time interval is reached or the maximum bytes limit is reached."
  val MetadataSnapshotMaxIntervalMsDoc = "This is the maximum number of milliseconds to wait to generate a snapshot " +
    "if there are committed records in the log that are not included in the latest snapshot. A value of zero disables " +
    s"time based snapshot generation. The default value is ${Defaults.MetadataSnapshotMaxIntervalMs}. To generate " +
    s"snapshots based on the number of metadata bytes, see the <code>$MetadataSnapshotMaxNewRecordBytesProp</code> " +
    "configuration. The Kafka node will generate a snapshot when either the maximum time interval is reached or the " +
    "maximum bytes limit is reached."
  val MetadataMaxIdleIntervalMsDoc = "This configuration controls how often the active " +
    "controller should write no-op records to the metadata partition. If the value is 0, no-op records " +
    s"are not appended to the metadata partition. The default value is ${Defaults.MetadataMaxIdleIntervalMs}";
  val ControllerListenerNamesDoc = "A comma-separated list of the names of the listeners used by the controller. This is required " +
    "if running in KRaft mode. When communicating with the controller quorum, the broker will always use the first listener in this list.\n " +
    "Note: The ZK-based controller should not set this configuration."
  val SaslMechanismControllerProtocolDoc = "SASL mechanism used for communication with controllers. Default is GSSAPI."
  val MetadataLogSegmentBytesDoc = "The maximum size of a single metadata log file."
  val MetadataLogSegmentMinBytesDoc = "Override the minimum size for a single metadata log file. This should be used for testing only."

  val MetadataLogSegmentMillisDoc = "The maximum time before a new metadata log file is rolled out (in milliseconds)."
  val MetadataMaxRetentionBytesDoc = "The maximum combined size of the metadata log and snapshots before deleting old " +
    "snapshots and log files. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit."
  val MetadataMaxRetentionMillisDoc = "The number of milliseconds to keep a metadata log file or snapshot before " +
    "deleting it. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit."

  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameDoc = s"The fully qualified name of a class that implements <code>${classOf[Authorizer].getName}</code>" +
    " interface, which is used by the broker for authorization."
  val EarlyStartListenersDoc = "A comma-separated list of listener names which may be started before the authorizer has finished " +
   "initialization. This is useful when the authorizer is dependent on the cluster itself for bootstrapping, as is the case for " +
   "the StandardAuthorizer (which stores ACLs in the metadata log.) By default, all listeners included in controller.listener.names " +
   "will also be early start listeners. A listener should not appear in this list if it accepts external traffic."

  /** ********* Socket Server Configuration ***********/
  val ListenersDoc = "Listener List - Comma-separated list of URIs we will listen on and the listener names." +
    s" If the listener name is not a security protocol, <code>$ListenerSecurityProtocolMapProp</code> must also be set.\n" +
    " Listener names and port numbers must be unique.\n" +
    " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
    " Leave hostname empty to bind to default interface.\n" +
    " Examples of legal listener lists:\n" +
    " PLAINTEXT://myhost:9092,SSL://:9091\n" +
    " CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093\n"
  val AdvertisedListenersDoc = s"Listeners to publish to ZooKeeper for clients to use, if different than the <code>$ListenersProp</code> config property." +
    " In IaaS environments, this may need to be different from the interface to which the broker binds." +
    s" If this is not set, the value for <code>$ListenersProp</code> will be used." +
    s" Unlike <code>$ListenersProp</code>, it is not valid to advertise the 0.0.0.0 meta-address.\n" +
    s" Also unlike <code>$ListenersProp</code>, there can be duplicated ports in this property," +
    " so that one listener can be configured to advertise another listener's address." +
    " This can be useful in some cases where external load balancers are used."
  val ListenerSecurityProtocolMapDoc = "Map between listener names and security protocols. This must be defined for " +
    "the same security protocol to be usable in more than one port or IP. For example, internal and " +
    "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners " +
    "with names INTERNAL and EXTERNAL and this property as: `INTERNAL:SSL,EXTERNAL:SSL`. As shown, key and value are " +
    "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. " +
    "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
    "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the " +
    "INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> would be set. " +
    "If the config for the listener name is not set, the config will fallback to the generic config (i.e. <code>ssl.keystore.location</code>). " +
    "Note that in KRaft a default mapping from the listener names defined by <code>controller.listener.names</code> to PLAINTEXT " +
    "is assumed if no explicit mapping is provided and no other security protocol is in use."
  val controlPlaneListenerNameDoc = "Name of listener used for communication between controller and brokers. " +
    s"Broker will use the $ControlPlaneListenerNameProp to locate the endpoint in $ListenersProp list, to listen for connections from the controller. " +
    "For example, if a broker's config is :\n" +
    "listeners = INTERNAL://192.1.1.8:9092, EXTERNAL://10.1.1.5:9093, CONTROLLER://192.1.1.8:9094\n" +
    "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL\n" +
    "control.plane.listener.name = CONTROLLER\n" +
    "On startup, the broker will start listening on \"192.1.1.8:9094\" with security protocol \"SSL\".\n" +
    s"On controller side, when it discovers a broker's published endpoints through zookeeper, it will use the $ControlPlaneListenerNameProp " +
    "to find the endpoint, which it will use to establish connection to the broker.\n" +
    "For example, if the broker's published endpoints on zookeeper are :\n" +
    "\"endpoints\" : [\"INTERNAL://broker1.example.com:9092\",\"EXTERNAL://broker1.example.com:9093\",\"CONTROLLER://broker1.example.com:9094\"]\n" +
    " and the controller's config is :\n" +
    "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL\n" +
    "control.plane.listener.name = CONTROLLER\n" +
    "then controller will use \"broker1.example.com:9094\" with security protocol \"SSL\" to connect to the broker.\n" +
    "If not explicitly configured, the default value will be null and there will be no dedicated endpoints for controller connections.\n" +
    s"If explicitly configured, the value cannot be the same as the value of <code>$InterBrokerListenerNameProp</code>."

  val SocketSendBufferBytesDoc = "The SO_SNDBUF buffer of the socket server sockets. If the value is -1, the OS default will be used."
  val SocketReceiveBufferBytesDoc = "The SO_RCVBUF buffer of the socket server sockets. If the value is -1, the OS default will be used."
  val SocketRequestMaxBytesDoc = "The maximum number of bytes in a socket request"
  val SocketListenBacklogSizeDoc = "The maximum number of pending connections on the socket. " +
    "In Linux, you may also need to configure `somaxconn` and `tcp_max_syn_backlog` kernel parameters " +
    "accordingly to make the configuration takes effect."
  val MaxConnectionsPerIpDoc = "The maximum number of connections we allow from each ip address. This can be set to 0 if there are overrides " +
    s"configured using $MaxConnectionsPerIpOverridesProp property. New connections from the ip address are dropped if the limit is reached."
  val MaxConnectionsPerIpOverridesDoc = "A comma-separated list of per-ip or hostname overrides to the default maximum number of connections. " +
    "An example value is \"hostName:100,127.0.0.1:200\""
  val MaxConnectionsDoc = "The maximum number of connections we allow in the broker at any time. This limit is applied in addition " +
    s"to any per-ip limits configured using $MaxConnectionsPerIpProp. Listener-level limits may also be configured by prefixing the " +
    s"config name with the listener prefix, for example, <code>listener.name.internal.$MaxConnectionsProp</code>. Broker-wide limit " +
    "should be configured based on broker capacity while listener limits should be configured based on application requirements. " +
    "New connections are blocked if either the listener or broker limit is reached. Connections on the inter-broker listener are " +
    "permitted even if broker-wide limit is reached. The least recently used connection on another listener will be closed in this case."
  val MaxConnectionCreationRateDoc = "The maximum connection creation rate we allow in the broker at any time. Listener-level limits " +
    s"may also be configured by prefixing the config name with the listener prefix, for example, <code>listener.name.internal.$MaxConnectionCreationRateProp</code>." +
    "Broker-wide connection rate limit should be configured based on broker capacity while listener limits should be configured based on " +
    "application requirements. New connections will be throttled if either the listener or the broker limit is reached, with the exception " +
    "of inter-broker listener. Connections on the inter-broker listener will be throttled only when the listener-level rate limit is reached."
  val ConnectionsMaxIdleMsDoc = "Idle connections timeout: the server socket processor threads close the connections that idle more than this"
  val FailedAuthenticationDelayMsDoc = "Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure. " +
    s"This must be configured to be less than $ConnectionsMaxIdleMsProp to prevent connection timeout."
  /************* Rack Configuration **************/
  val RackDoc = "Rack of the broker. This will be used in rack aware replication assignment for fault tolerance. Examples: `RACK1`, `us-east-1d`"
  /** ********* Log Configuration ***********/
  val NumPartitionsDoc = "The default number of log partitions per topic"
  val LogDirDoc = "The directory in which the log data is kept (supplemental for " + LogDirsProp + " property)"
  val LogDirsDoc = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LogDirProp + " is used."
  val LogSegmentBytesDoc = "The maximum size of a single log file"
  val LogRollTimeMillisDoc = "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in " + LogRollTimeHoursProp + " is used"
  val LogRollTimeHoursDoc = "The maximum time before a new log segment is rolled out (in hours), secondary to " + LogRollTimeMillisProp + " property"

  val LogRollTimeJitterMillisDoc = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in " + LogRollTimeJitterHoursProp + " is used"
  val LogRollTimeJitterHoursDoc = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to " + LogRollTimeJitterMillisProp + " property"

  val LogRetentionTimeMillisDoc = "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in " + LogRetentionTimeMinutesProp + " is used. If set to -1, no time limit is applied."
  val LogRetentionTimeMinsDoc = "The number of minutes to keep a log file before deleting it (in minutes), secondary to " + LogRetentionTimeMillisProp + " property. If not set, the value in " + LogRetentionTimeHoursProp + " is used"
  val LogRetentionTimeHoursDoc = "The number of hours to keep a log file before deleting it (in hours), tertiary to " + LogRetentionTimeMillisProp + " property"

  val LogRetentionBytesDoc = "The maximum size of the log before deleting it"
  val LogCleanupIntervalMsDoc = "The frequency in milliseconds that the log cleaner checks whether any log is eligible for deletion"
  val LogCleanupPolicyDoc = "The default cleanup policy for segments beyond the retention window. A comma separated list of valid policies. Valid policies are: \"delete\" and \"compact\""
  val LogCleanerThreadsDoc = "The number of background threads to use for log cleaning"
  val LogCleanerIoMaxBytesPerSecondDoc = "The log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average"
  val LogCleanerDedupeBufferSizeDoc = "The total memory used for log deduplication across all cleaner threads"
  val LogCleanerIoBufferSizeDoc = "The total memory used for log cleaner I/O buffers across all cleaner threads"
  val LogCleanerDedupeBufferLoadFactorDoc = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value " +
  "will allow more log to be cleaned at once but will lead to more hash collisions"
  val LogCleanerBackoffMsDoc = "The amount of time to sleep when there are no logs to clean"
  val LogCleanerMinCleanRatioDoc = "The minimum ratio of dirty log to total log for a log to eligible for cleaning. " +
    "If the " + LogCleanerMaxCompactionLagMsProp + " or the " + LogCleanerMinCompactionLagMsProp +
    " configurations are also specified, then the log compactor considers the log eligible for compaction " +
    "as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) " +
    "records for at least the " + LogCleanerMinCompactionLagMsProp + " duration, or (ii) if the log has had " +
    "dirty (uncompacted) records for at most the " + LogCleanerMaxCompactionLagMsProp + " period."
  val LogCleanerEnableDoc = "Enable the log cleaner process to run on the server. Should be enabled if using any topics with a cleanup.policy=compact including the internal offsets topic. If disabled those topics will not be compacted and continually grow in size."
  val LogCleanerDeleteRetentionMsDoc = "The amount of time to retain delete tombstone markers for log compacted topics. This setting also gives a bound " +
    "on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete " +
    "tombstones may be collected before they complete their scan).";
  val LogCleanerMinCompactionLagMsDoc = "The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted."
  val LogCleanerMaxCompactionLagMsDoc = "The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted."
  val LogIndexSizeMaxBytesDoc = "The maximum size in bytes of the offset index"
  val LogIndexIntervalBytesDoc = "The interval with which we add an entry to the offset index"
  val LogFlushIntervalMessagesDoc = "The number of messages accumulated on a log partition before messages are flushed to disk "
  val LogDeleteDelayMsDoc = "The amount of time to wait before deleting a file from the filesystem"
  val LogFlushSchedulerIntervalMsDoc = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk"
  val LogFlushIntervalMsDoc = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in " + LogFlushSchedulerIntervalMsProp + " is used"
  val LogFlushOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point"
  val LogFlushStartOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of log start offset"
  val LogPreAllocateEnableDoc = "Should pre allocate file when create new segment? If you are using Kafka on Windows, you probably need to set it to true."
  val LogMessageFormatVersionDoc = "Specify the message format version the broker will use to append messages to the logs. The value should be a valid MetadataVersion. " +
    "Some examples are: 0.8.2, 0.9.0.0, 0.10.0, check MetadataVersion for more details. By setting a particular message format version, the " +
    "user is certifying that all the existing messages on disk are smaller or equal than the specified version. Setting this value incorrectly " +
    "will cause consumers with older versions to break as they will receive messages with a format that they don't understand."

  val LogMessageTimestampTypeDoc = "Define whether the timestamp in the message is message create time or log append time. The value should be either " +
    "`CreateTime` or `LogAppendTime`"

  val LogMessageTimestampDifferenceMaxMsDoc = "The maximum difference allowed between the timestamp when a broker receives " +
    "a message and the timestamp specified in the message. If log.message.timestamp.type=CreateTime, a message will be rejected " +
    "if the difference in timestamp exceeds this threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime." +
    "The maximum timestamp difference allowed should be no greater than log.retention.ms to avoid unnecessarily frequent log rolling."
  val NumRecoveryThreadsPerDataDirDoc = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown"
  val AutoCreateTopicsEnableDoc = "Enable auto creation of topic on the server"
  val MinInSyncReplicasDoc = "When a producer sets acks to \"all\" (or \"-1\"), " +
    "min.insync.replicas specifies the minimum number of replicas that must acknowledge " +
    "a write for the write to be considered successful. If this minimum cannot be met, " +
    "then the producer will raise an exception (either NotEnoughReplicas or " +
    "NotEnoughReplicasAfterAppend).<br>When used together, min.insync.replicas and acks " +
    "allow you to enforce greater durability guarantees. A typical scenario would be to " +
    "create a topic with a replication factor of 3, set min.insync.replicas to 2, and " +
    "produce with acks of \"all\". This will ensure that the producer raises an exception " +
    "if a majority of replicas do not receive a write."

  val CreateTopicPolicyClassNameDoc = "The create topic policy class that should be used for validation. The class should " +
    "implement the <code>org.apache.kafka.server.policy.CreateTopicPolicy</code> interface."
  val AlterConfigPolicyClassNameDoc = "The alter configs policy class that should be used for validation. The class should " +
    "implement the <code>org.apache.kafka.server.policy.AlterConfigPolicy</code> interface."
  val LogMessageDownConversionEnableDoc = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC;

  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsDoc = "The socket timeout for controller-to-broker channels"
  val ControllerMessageQueueSizeDoc = "The buffer size for controller-to-broker-channels"
  val DefaultReplicationFactorDoc = "The default replication factors for automatically created topics"
  val ReplicaLagTimeMaxMsDoc = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
  " the leader will remove the follower from isr"
  val ReplicaSocketTimeoutMsDoc = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms"
  val ReplicaSocketReceiveBufferBytesDoc = "The socket receive buffer for network requests"
  val ReplicaFetchMaxBytesDoc = "The number of bytes of messages to attempt to fetch for each partition. This is not an absolute maximum, " +
    "if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch will still be returned " +
    "to ensure that progress can be made. The maximum record batch size accepted by the broker is defined via " +
    "<code>message.max.bytes</code> (broker config) or <code>max.message.bytes</code> (topic config)."
  val ReplicaFetchWaitMaxMsDoc = "The maximum wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
  "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics"
  val ReplicaFetchMinBytesDoc = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to <code>replica.fetch.wait.max.ms</code> (broker config)."
  val ReplicaFetchResponseMaxBytesDoc = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
    "and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch " +
    "will still be returned to ensure that progress can be made. As such, this is not an absolute maximum. The maximum " +
    "record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
    "<code>max.message.bytes</code> (topic config)."
  val NumReplicaFetchersDoc = "Number of fetcher threads used to replicate records from each source broker. The total number of fetchers " +
  "on each broker is bound by <code>num.replica.fetchers</code> multiplied by the number of brokers in the cluster." +
  "Increasing this value can increase the degree of I/O parallelism in the follower and leader broker at the cost " +
  "of higher CPU and memory utilization."
  val ReplicaFetchBackoffMsDoc = "The amount of time to sleep when fetch partition error occurs."
  val ReplicaHighWatermarkCheckpointIntervalMsDoc = "The frequency with which the high watermark is saved out to disk"
  val FetchPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the fetch request purgatory"
  val ProducerPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the producer request purgatory"
  val DeleteRecordsPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the delete records request purgatory"
  val AutoLeaderRebalanceEnableDoc = "Enables auto leader balancing. A background thread checks the distribution of partition leaders at regular intervals, configurable by `leader.imbalance.check.interval.seconds`. If the leader imbalance exceeds `leader.imbalance.per.broker.percentage`, leader rebalance to the preferred leader for partitions is triggered."
  val LeaderImbalancePerBrokerPercentageDoc = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage."
  val LeaderImbalanceCheckIntervalSecondsDoc = "The frequency with which the partition rebalance check is triggered by the controller"
  val UncleanLeaderElectionEnableDoc = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss"
  val InterBrokerSecurityProtocolDoc = "Security protocol used to communicate between brokers. Valid values are: " +
    s"${SecurityProtocol.names.asScala.mkString(", ")}. It is an error to set this and $InterBrokerListenerNameProp " +
    "properties at the same time."
  val InterBrokerProtocolVersionDoc = "Specify which version of the inter-broker protocol will be used.\n" +
  " This is typically bumped after all brokers were upgraded to a new version.\n" +
  " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check MetadataVersion for the full list."
  val InterBrokerListenerNameDoc = s"Name of listener used for communication between brokers. If this is unset, the listener name is defined by $InterBrokerSecurityProtocolProp. " +
    s"It is an error to set this and $InterBrokerSecurityProtocolProp properties at the same time."
  val ReplicaSelectorClassDoc = "The fully qualified class name that implements ReplicaSelector. This is used by the broker to find the preferred read replica. By default, we use an implementation that returns the leader."
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesDoc = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens"
  val ControlledShutdownRetryBackoffMsDoc = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying."
  val ControlledShutdownEnableDoc = "Enable controlled shutdown of the server"
  /** ********* Group coordinator configuration ***********/
  val GroupMinSessionTimeoutMsDoc = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources."
  val GroupMaxSessionTimeoutMsDoc = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures."
  val GroupInitialRebalanceDelayMsDoc = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins."
  val GroupMaxSizeDoc = "The maximum number of consumers that a single consumer group can accommodate."
  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSizeDoc = "The maximum size for a metadata entry associated with an offset commit"
  val OffsetsLoadBufferSizeDoc = "Batch size for reading from the offsets segments when loading offsets into the cache (soft-limit, overridden if records are too large)."
  val OffsetsTopicReplicationFactorDoc = "The replication factor for the offsets topic (set higher to ensure availability). " +
  "Internal topic creation will fail until the cluster size meets this replication factor requirement."
  val OffsetsTopicPartitionsDoc = "The number of partitions for the offset commit topic (should not change after deployment)"
  val OffsetsTopicSegmentBytesDoc = "The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads"
  val OffsetsTopicCompressionCodecDoc = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits"
  val OffsetsRetentionMinutesDoc = "For subscribed consumers, committed offset of a specific partition will be expired and discarded when 1) this retention period has elapsed after the consumer group loses all its consumers (i.e. becomes empty); " +
    "2) this retention period has elapsed since the last time an offset is committed for the partition and the group is no longer subscribed to the corresponding topic. " +
    "For standalone consumers (using manual assignment), offsets will be expired after this retention period has elapsed since the time of last commit. " +
    "Note that when a group is deleted via the delete-group request, its committed offsets will also be deleted without extra retention period; " +
    "also when a topic is deleted via the delete-topic request, upon propagated metadata update any group's committed offsets for that topic will also be deleted without extra retention period."
  val OffsetsRetentionCheckIntervalMsDoc = "Frequency at which to check for stale offsets"
  val OffsetCommitTimeoutMsDoc = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
  "or this timeout is reached. This is similar to the producer request timeout."
  val OffsetCommitRequiredAcksDoc = "The required acks before the commit can be accepted. In general, the default (-1) should not be overridden"
  /** ********* Transaction management configuration ***********/
  val TransactionalIdExpirationMsDoc = "The time in ms that the transaction coordinator will wait without receiving any transaction status updates " +
    "for the current transaction before expiring its transactional id. Transactional IDs will not expire while a the transaction is still ongoing."
  val TransactionsMaxTimeoutMsDoc = "The maximum allowed timeout for transactions. " +
    "If a client’s requested transaction time exceed this, then the broker will return an error in InitProducerIdRequest. This prevents a client from too large of a timeout, which can stall consumers reading from topics included in the transaction."
  val TransactionsTopicMinISRDoc = "Overridden " + MinInSyncReplicasProp + " config for the transaction topic."
  val TransactionsLoadBufferSizeDoc = "Batch size for reading from the transaction log segments when loading producer ids and transactions into the cache (soft-limit, overridden if records are too large)."
  val TransactionsTopicReplicationFactorDoc = "The replication factor for the transaction topic (set higher to ensure availability). " +
    "Internal topic creation will fail until the cluster size meets this replication factor requirement."
  val TransactionsTopicPartitionsDoc = "The number of partitions for the transaction topic (should not change after deployment)."
  val TransactionsTopicSegmentBytesDoc = "The transaction topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads"
  val TransactionsAbortTimedOutTransactionsIntervalMsDoc = "The interval at which to rollback transactions that have timed out"
  val TransactionsRemoveExpiredTransactionsIntervalMsDoc = "The interval at which to remove transactions that have expired due to <code>transactional.id.expiration.ms</code> passing"

  val ProducerIdExpirationMsDoc = "The time in ms that a topic partition leader will wait before expiring producer IDs. Producer IDs will not expire while a transaction associated to them is still ongoing. " +
    "Note that producer IDs may expire sooner if the last write from the producer ID is deleted due to the topic's retention settings. Setting this value the same or higher than " +
    "<code>delivery.timeout.ms</code> can help prevent expiration during retries and protect against message duplication, but the default should be reasonable for most use cases."
  val ProducerIdExpirationCheckIntervalMsDoc = "The interval at which to remove producer IDs that have expired due to <code>producer.id.expiration.ms</code> passing"

  /** ********* Fetch Configuration **************/
  val MaxIncrementalFetchSessionCacheSlotsDoc = "The maximum number of incremental fetch sessions that we will maintain."
  val FetchMaxBytesDoc = "The maximum number of bytes we will return for a fetch request. Must be at least 1024."

  /** ********* Quota Configuration ***********/
  val NumQuotaSamplesDoc = "The number of samples to retain in memory for client quotas"
  val NumReplicationQuotaSamplesDoc = "The number of samples to retain in memory for replication quotas"
  val NumAlterLogDirsReplicationQuotaSamplesDoc = "The number of samples to retain in memory for alter log dirs replication quotas"
  val NumControllerQuotaSamplesDoc = "The number of samples to retain in memory for controller mutation quotas"
  val QuotaWindowSizeSecondsDoc = "The time span of each sample for client quotas"
  val ReplicationQuotaWindowSizeSecondsDoc = "The time span of each sample for replication quotas"
  val AlterLogDirsReplicationQuotaWindowSizeSecondsDoc = "The time span of each sample for alter log dirs replication quotas"
  val ControllerQuotaWindowSizeSecondsDoc = "The time span of each sample for controller mutations quotas"

  val ClientQuotaCallbackClassDoc = "The fully qualified name of a class that implements the ClientQuotaCallback interface, " +
    "which is used to determine quota limits applied to client requests. By default, the &lt;user&gt; and &lt;client-id&gt; " +
    "quotas that are stored in ZooKeeper are applied. For any given request, the most specific quota that matches the user principal " +
    "of the session and the client-id of the request is applied."

  val DeleteTopicEnableDoc = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off"
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
  "('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
  "'producer' which means retain the original compression codec set by the producer."

  /** ********* Kafka Metrics Configuration ***********/
  val MetricSampleWindowMsDoc = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC
  val MetricNumSamplesDoc = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC
  val MetricReporterClassesDoc = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC
  val MetricRecordingLevelDoc = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC
  val AutoIncludeJmxReporterDoc = CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC


  /** ********* Kafka Yammer Metrics Reporter Configuration ***********/
  val KafkaMetricsReporterClassesDoc = "A list of classes to use as Yammer metrics custom reporters." +
    " The reporters should implement <code>kafka.metrics.KafkaMetricsReporter</code> trait. If a client wants" +
    " to expose JMX operations on a custom reporter, the custom reporter needs to additionally implement an MBean" +
    " trait that extends <code>kafka.metrics.KafkaMetricsReporterMBean</code> trait so that the registered MBean is compliant with" +
    " the standard MBean convention."

  val KafkaMetricsPollingIntervalSecondsDoc = s"The metrics polling interval (in seconds) which can be used" +
    s" in $KafkaMetricsReporterClassesProp implementations."

  /** ******** Common Security Configuration *************/
  val PrincipalBuilderClassDoc = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC
  val ConnectionsMaxReauthMsDoc = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC
  val SaslServerMaxReceiveSizeDoc = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC
  val securityProviderClassDoc = SecurityConfig.SECURITY_PROVIDERS_DOC

  /** ********* SSL Configuration ****************/
  val SslProtocolDoc = SslConfigs.SSL_PROTOCOL_DOC
  val SslProviderDoc = SslConfigs.SSL_PROVIDER_DOC
  val SslCipherSuitesDoc = SslConfigs.SSL_CIPHER_SUITES_DOC
  val SslEnabledProtocolsDoc = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC
  val SslKeystoreTypeDoc = SslConfigs.SSL_KEYSTORE_TYPE_DOC
  val SslKeystoreLocationDoc = SslConfigs.SSL_KEYSTORE_LOCATION_DOC
  val SslKeystorePasswordDoc = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC
  val SslKeyPasswordDoc = SslConfigs.SSL_KEY_PASSWORD_DOC
  val SslKeystoreKeyDoc = SslConfigs.SSL_KEYSTORE_KEY_DOC
  val SslKeystoreCertificateChainDoc = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC
  val SslTruststoreTypeDoc = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC
  val SslTruststorePasswordDoc = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC
  val SslTruststoreLocationDoc = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC
  val SslTruststoreCertificatesDoc = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC
  val SslKeyManagerAlgorithmDoc = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC
  val SslTrustManagerAlgorithmDoc = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC
  val SslEndpointIdentificationAlgorithmDoc = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
  val SslSecureRandomImplementationDoc = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC
  val SslClientAuthDoc = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC
  val SslPrincipalMappingRulesDoc = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC
  val SslEngineFactoryClassDoc = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC

  /** ********* Sasl Configuration ****************/
  val SaslMechanismInterBrokerProtocolDoc = "SASL mechanism used for inter-broker communication. Default is GSSAPI."
  val SaslJaasConfigDoc = SaslConfigs.SASL_JAAS_CONFIG_DOC
  val SaslEnabledMechanismsDoc = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC
  val SaslServerCallbackHandlerClassDoc = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC
  val SaslClientCallbackHandlerClassDoc = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC
  val SaslLoginClassDoc = SaslConfigs.SASL_LOGIN_CLASS_DOC
  val SaslLoginCallbackHandlerClassDoc = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC
  val SaslKerberosServiceNameDoc = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC
  val SaslKerberosKinitCmdDoc = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC
  val SaslKerberosTicketRenewWindowFactorDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
  val SaslKerberosTicketRenewJitterDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
  val SaslKerberosMinTimeBeforeReloginDoc = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
  val SaslKerberosPrincipalToLocalRulesDoc = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC
  val SaslLoginRefreshWindowFactorDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
  val SaslLoginRefreshWindowJitterDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
  val SaslLoginRefreshMinPeriodSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
  val SaslLoginRefreshBufferSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC

  val SaslLoginConnectTimeoutMsDoc = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC
  val SaslLoginReadTimeoutMsDoc = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC
  val SaslLoginRetryBackoffMaxMsDoc = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC
  val SaslLoginRetryBackoffMsDoc = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC
  val SaslOAuthBearerScopeClaimNameDoc = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC
  val SaslOAuthBearerSubClaimNameDoc = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC
  val SaslOAuthBearerTokenEndpointUrlDoc = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC
  val SaslOAuthBearerJwksEndpointUrlDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC
  val SaslOAuthBearerJwksEndpointRefreshMsDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC
  val SaslOAuthBearerJwksEndpointRetryBackoffMaxMsDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC
  val SaslOAuthBearerJwksEndpointRetryBackoffMsDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC
  val SaslOAuthBearerClockSkewSecondsDoc = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC
  val SaslOAuthBearerExpectedAudienceDoc = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC
  val SaslOAuthBearerExpectedIssuerDoc = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC

  /** ********* Delegation Token Configuration ****************/
  val DelegationTokenSecretKeyAliasDoc = s"DEPRECATED: An alias for $DelegationTokenSecretKeyProp, which should be used instead of this config."
  val DelegationTokenSecretKeyDoc = "Secret key to generate and verify delegation tokens. The same key must be configured across all the brokers. " +
    " If the key is not set or set to empty string, brokers will disable the delegation token support."
  val DelegationTokenMaxLifeTimeDoc = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days."
  val DelegationTokenExpiryTimeMsDoc = "The token validity time in miliseconds before the token needs to be renewed. Default value 1 day."
  val DelegationTokenExpiryCheckIntervalDoc = "Scan interval to remove expired delegation tokens."

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderSecretDoc = "The secret used for encoding dynamically configured passwords for this broker."
  val PasswordEncoderOldSecretDoc = "The old secret that was used for encoding dynamically configured passwords. " +
    "This is required only when the secret is updated. If specified, all dynamically encoded passwords are " +
    s"decoded using this old secret and re-encoded using $PasswordEncoderSecretProp when broker starts up."
  val PasswordEncoderKeyFactoryAlgorithmDoc = "The SecretKeyFactory algorithm used for encoding dynamically configured passwords. " +
    "Default is PBKDF2WithHmacSHA512 if available and PBKDF2WithHmacSHA1 otherwise."
  val PasswordEncoderCipherAlgorithmDoc = "The Cipher algorithm used for encoding dynamically configured passwords."
  val PasswordEncoderKeyLengthDoc =  "The key length used for encoding dynamically configured passwords."
  val PasswordEncoderIterationsDoc =  "The iteration count used for encoding dynamically configured passwords."

  @nowarn("cat=deprecation")
  private[server] val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Range._
    import ConfigDef.Type._
    import ConfigDef.ValidString._

    new ConfigDef()

      /** ********* Zookeeper Configuration ***********/
      .define(ZkConnectProp, STRING, null, HIGH, ZkConnectDoc)
      .define(ZkSessionTimeoutMsProp, INT, Defaults.ZkSessionTimeoutMs, HIGH, ZkSessionTimeoutMsDoc)
      .define(ZkConnectionTimeoutMsProp, INT, null, HIGH, ZkConnectionTimeoutMsDoc)
      .define(ZkEnableSecureAclsProp, BOOLEAN, Defaults.ZkEnableSecureAcls, HIGH, ZkEnableSecureAclsDoc)
      .define(ZkMaxInFlightRequestsProp, INT, Defaults.ZkMaxInFlightRequests, atLeast(1), HIGH, ZkMaxInFlightRequestsDoc)
      .define(ZkSslClientEnableProp, BOOLEAN, Defaults.ZkSslClientEnable, MEDIUM, ZkSslClientEnableDoc)
      .define(ZkClientCnxnSocketProp, STRING, null, MEDIUM, ZkClientCnxnSocketDoc)
      .define(ZkSslKeyStoreLocationProp, STRING, null, MEDIUM, ZkSslKeyStoreLocationDoc)
      .define(ZkSslKeyStorePasswordProp, PASSWORD, null, MEDIUM, ZkSslKeyStorePasswordDoc)
      .define(ZkSslKeyStoreTypeProp, STRING, null, MEDIUM, ZkSslKeyStoreTypeDoc)
      .define(ZkSslTrustStoreLocationProp, STRING, null, MEDIUM, ZkSslTrustStoreLocationDoc)
      .define(ZkSslTrustStorePasswordProp, PASSWORD, null, MEDIUM, ZkSslTrustStorePasswordDoc)
      .define(ZkSslTrustStoreTypeProp, STRING, null, MEDIUM, ZkSslTrustStoreTypeDoc)
      .define(ZkSslProtocolProp, STRING, Defaults.ZkSslProtocol, LOW, ZkSslProtocolDoc)
      .define(ZkSslEnabledProtocolsProp, LIST, null, LOW, ZkSslEnabledProtocolsDoc)
      .define(ZkSslCipherSuitesProp, LIST, null, LOW, ZkSslCipherSuitesDoc)
      .define(ZkSslEndpointIdentificationAlgorithmProp, STRING, Defaults.ZkSslEndpointIdentificationAlgorithm, LOW, ZkSslEndpointIdentificationAlgorithmDoc)
      .define(ZkSslCrlEnableProp, BOOLEAN, Defaults.ZkSslCrlEnable, LOW, ZkSslCrlEnableDoc)
      .define(ZkSslOcspEnableProp, BOOLEAN, Defaults.ZkSslOcspEnable, LOW, ZkSslOcspEnableDoc)

      /** ********* General Configuration ***********/
      .define(BrokerIdGenerationEnableProp, BOOLEAN, Defaults.BrokerIdGenerationEnable, MEDIUM, BrokerIdGenerationEnableDoc)
      .define(MaxReservedBrokerIdProp, INT, Defaults.MaxReservedBrokerId, atLeast(0), MEDIUM, MaxReservedBrokerIdDoc)
      .define(BrokerIdProp, INT, Defaults.BrokerId, HIGH, BrokerIdDoc)
      .define(MessageMaxBytesProp, INT, Defaults.MessageMaxBytes, atLeast(0), HIGH, MessageMaxBytesDoc)
      .define(NumNetworkThreadsProp, INT, Defaults.NumNetworkThreads, atLeast(1), HIGH, NumNetworkThreadsDoc)
      .define(NumIoThreadsProp, INT, Defaults.NumIoThreads, atLeast(1), HIGH, NumIoThreadsDoc)
      .define(NumReplicaAlterLogDirsThreadsProp, INT, null, HIGH, NumReplicaAlterLogDirsThreadsDoc)
      .define(BackgroundThreadsProp, INT, Defaults.BackgroundThreads, atLeast(1), HIGH, BackgroundThreadsDoc)
      .define(QueuedMaxRequestsProp, INT, Defaults.QueuedMaxRequests, atLeast(1), HIGH, QueuedMaxRequestsDoc)
      .define(QueuedMaxBytesProp, LONG, Defaults.QueuedMaxRequestBytes, MEDIUM, QueuedMaxRequestBytesDoc)
      .define(RequestTimeoutMsProp, INT, Defaults.RequestTimeoutMs, HIGH, RequestTimeoutMsDoc)
      .define(ConnectionSetupTimeoutMsProp, LONG, Defaults.ConnectionSetupTimeoutMs, MEDIUM, ConnectionSetupTimeoutMsDoc)
      .define(ConnectionSetupTimeoutMaxMsProp, LONG, Defaults.ConnectionSetupTimeoutMaxMs, MEDIUM, ConnectionSetupTimeoutMaxMsDoc)

      /*
       * KRaft mode configs.
       */
      .define(MetadataSnapshotMaxNewRecordBytesProp, LONG, Defaults.MetadataSnapshotMaxNewRecordBytes, atLeast(1), HIGH, MetadataSnapshotMaxNewRecordBytesDoc)
      .define(MetadataSnapshotMaxIntervalMsProp, LONG, Defaults.MetadataSnapshotMaxIntervalMs, atLeast(0), HIGH, MetadataSnapshotMaxIntervalMsDoc)

      /*
       * KRaft mode private configs. Note that these configs are defined as internal. We will make them public in the 3.0.0 release.
       */
      .define(ProcessRolesProp, LIST, Collections.emptyList(), ValidList.in("broker", "controller"), HIGH, ProcessRolesDoc)
      .define(NodeIdProp, INT, Defaults.EmptyNodeId, null, HIGH, NodeIdDoc)
      .define(InitialBrokerRegistrationTimeoutMsProp, INT, Defaults.InitialBrokerRegistrationTimeoutMs, null, MEDIUM, InitialBrokerRegistrationTimeoutMsDoc)
      .define(BrokerHeartbeatIntervalMsProp, INT, Defaults.BrokerHeartbeatIntervalMs, null, MEDIUM, BrokerHeartbeatIntervalMsDoc)
      .define(BrokerSessionTimeoutMsProp, INT, Defaults.BrokerSessionTimeoutMs, null, MEDIUM, BrokerSessionTimeoutMsDoc)
      .define(ControllerListenerNamesProp, STRING, null, null, HIGH, ControllerListenerNamesDoc)
      .define(SaslMechanismControllerProtocolProp, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, SaslMechanismControllerProtocolDoc)
      .define(MetadataLogDirProp, STRING, null, null, HIGH, MetadataLogDirDoc)
      .define(MetadataLogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(Records.LOG_OVERHEAD), HIGH, MetadataLogSegmentBytesDoc)
      .defineInternal(MetadataLogSegmentMinBytesProp, INT, 8 * 1024 * 1024, atLeast(Records.LOG_OVERHEAD), HIGH, MetadataLogSegmentMinBytesDoc)
      .define(MetadataLogSegmentMillisProp, LONG, Defaults.LogRollHours * 60 * 60 * 1000L, null, HIGH, MetadataLogSegmentMillisDoc)
      .define(MetadataMaxRetentionBytesProp, LONG, Defaults.MetadataMaxRetentionBytes, null, HIGH, MetadataMaxRetentionBytesDoc)
      .define(MetadataMaxRetentionMillisProp, LONG, Defaults.LogRetentionHours * 60 * 60 * 1000L, null, HIGH, MetadataMaxRetentionMillisDoc)
      .define(MetadataMaxIdleIntervalMsProp, INT, Defaults.MetadataMaxIdleIntervalMs, atLeast(0), LOW, MetadataMaxIdleIntervalMsDoc)
      .define(MigrationEnabledProp, BOOLEAN, false, HIGH, "Enable ZK to KRaft migration")

      /************* Authorizer Configuration ***********/
      .define(AuthorizerClassNameProp, STRING, Defaults.AuthorizerClassName, new ConfigDef.NonNullValidator(), LOW, AuthorizerClassNameDoc)
      .define(EarlyStartListenersProp, STRING, null,  HIGH, EarlyStartListenersDoc)

      /** ********* Socket Server Configuration ***********/
      .define(ListenersProp, STRING, Defaults.Listeners, HIGH, ListenersDoc)
      .define(AdvertisedListenersProp, STRING, null, HIGH, AdvertisedListenersDoc)
      .define(ListenerSecurityProtocolMapProp, STRING, Defaults.ListenerSecurityProtocolMap, LOW, ListenerSecurityProtocolMapDoc)
      .define(ControlPlaneListenerNameProp, STRING, null, HIGH, controlPlaneListenerNameDoc)
      .define(SocketSendBufferBytesProp, INT, Defaults.SocketSendBufferBytes, HIGH, SocketSendBufferBytesDoc)
      .define(SocketReceiveBufferBytesProp, INT, Defaults.SocketReceiveBufferBytes, HIGH, SocketReceiveBufferBytesDoc)
      .define(SocketRequestMaxBytesProp, INT, Defaults.SocketRequestMaxBytes, atLeast(1), HIGH, SocketRequestMaxBytesDoc)
      .define(SocketListenBacklogSizeProp, INT, Defaults.SocketListenBacklogSize, atLeast(1), MEDIUM, SocketListenBacklogSizeDoc)
      .define(MaxConnectionsPerIpProp, INT, Defaults.MaxConnectionsPerIp, atLeast(0), MEDIUM, MaxConnectionsPerIpDoc)
      .define(MaxConnectionsPerIpOverridesProp, STRING, Defaults.MaxConnectionsPerIpOverrides, MEDIUM, MaxConnectionsPerIpOverridesDoc)
      .define(MaxConnectionsProp, INT, Defaults.MaxConnections, atLeast(0), MEDIUM, MaxConnectionsDoc)
      .define(MaxConnectionCreationRateProp, INT, Defaults.MaxConnectionCreationRate, atLeast(0), MEDIUM, MaxConnectionCreationRateDoc)
      .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, MEDIUM, ConnectionsMaxIdleMsDoc)
      .define(FailedAuthenticationDelayMsProp, INT, Defaults.FailedAuthenticationDelayMs, atLeast(0), LOW, FailedAuthenticationDelayMsDoc)

      /************ Rack Configuration ******************/
      .define(RackProp, STRING, null, MEDIUM, RackDoc)

      /** ********* Log Configuration ***********/
      .define(NumPartitionsProp, INT, Defaults.NumPartitions, atLeast(1), MEDIUM, NumPartitionsDoc)
      .define(LogDirProp, STRING, Defaults.LogDir, HIGH, LogDirDoc)
      .define(LogDirsProp, STRING, null, HIGH, LogDirsDoc)
      .define(LogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, LogSegmentBytesDoc)

      .define(LogRollTimeMillisProp, LONG, null, HIGH, LogRollTimeMillisDoc)
      .define(LogRollTimeHoursProp, INT, Defaults.LogRollHours, atLeast(1), HIGH, LogRollTimeHoursDoc)

      .define(LogRollTimeJitterMillisProp, LONG, null, HIGH, LogRollTimeJitterMillisDoc)
      .define(LogRollTimeJitterHoursProp, INT, Defaults.LogRollJitterHours, atLeast(0), HIGH, LogRollTimeJitterHoursDoc)

      .define(LogRetentionTimeMillisProp, LONG, null, HIGH, LogRetentionTimeMillisDoc)
      .define(LogRetentionTimeMinutesProp, INT, null, HIGH, LogRetentionTimeMinsDoc)
      .define(LogRetentionTimeHoursProp, INT, Defaults.LogRetentionHours, HIGH, LogRetentionTimeHoursDoc)

      .define(LogRetentionBytesProp, LONG, Defaults.LogRetentionBytes, HIGH, LogRetentionBytesDoc)
      .define(LogCleanupIntervalMsProp, LONG, Defaults.LogCleanupIntervalMs, atLeast(1), MEDIUM, LogCleanupIntervalMsDoc)
      .define(LogCleanupPolicyProp, LIST, Defaults.LogCleanupPolicy, ValidList.in(Defaults.Compact, Defaults.Delete), MEDIUM, LogCleanupPolicyDoc)
      .define(LogCleanerThreadsProp, INT, Defaults.LogCleanerThreads, atLeast(0), MEDIUM, LogCleanerThreadsDoc)
      .define(LogCleanerIoMaxBytesPerSecondProp, DOUBLE, Defaults.LogCleanerIoMaxBytesPerSecond, MEDIUM, LogCleanerIoMaxBytesPerSecondDoc)
      .define(LogCleanerDedupeBufferSizeProp, LONG, Defaults.LogCleanerDedupeBufferSize, MEDIUM, LogCleanerDedupeBufferSizeDoc)
      .define(LogCleanerIoBufferSizeProp, INT, Defaults.LogCleanerIoBufferSize, atLeast(0), MEDIUM, LogCleanerIoBufferSizeDoc)
      .define(LogCleanerDedupeBufferLoadFactorProp, DOUBLE, Defaults.LogCleanerDedupeBufferLoadFactor, MEDIUM, LogCleanerDedupeBufferLoadFactorDoc)
      .define(LogCleanerBackoffMsProp, LONG, Defaults.LogCleanerBackoffMs, atLeast(0), MEDIUM, LogCleanerBackoffMsDoc)
      .define(LogCleanerMinCleanRatioProp, DOUBLE, Defaults.LogCleanerMinCleanRatio, between(0, 1), MEDIUM, LogCleanerMinCleanRatioDoc)
      .define(LogCleanerEnableProp, BOOLEAN, Defaults.LogCleanerEnable, MEDIUM, LogCleanerEnableDoc)
      .define(LogCleanerDeleteRetentionMsProp, LONG, Defaults.LogCleanerDeleteRetentionMs, atLeast(0), MEDIUM, LogCleanerDeleteRetentionMsDoc)
      .define(LogCleanerMinCompactionLagMsProp, LONG, Defaults.LogCleanerMinCompactionLagMs, atLeast(0), MEDIUM, LogCleanerMinCompactionLagMsDoc)
      .define(LogCleanerMaxCompactionLagMsProp, LONG, Defaults.LogCleanerMaxCompactionLagMs, atLeast(1), MEDIUM, LogCleanerMaxCompactionLagMsDoc)
      .define(LogIndexSizeMaxBytesProp, INT, Defaults.LogIndexSizeMaxBytes, atLeast(4), MEDIUM, LogIndexSizeMaxBytesDoc)
      .define(LogIndexIntervalBytesProp, INT, Defaults.LogIndexIntervalBytes, atLeast(0), MEDIUM, LogIndexIntervalBytesDoc)
      .define(LogFlushIntervalMessagesProp, LONG, Defaults.LogFlushIntervalMessages, atLeast(1), HIGH, LogFlushIntervalMessagesDoc)
      .define(LogDeleteDelayMsProp, LONG, Defaults.LogDeleteDelayMs, atLeast(0), HIGH, LogDeleteDelayMsDoc)
      .define(LogFlushSchedulerIntervalMsProp, LONG, Defaults.LogFlushSchedulerIntervalMs, HIGH, LogFlushSchedulerIntervalMsDoc)
      .define(LogFlushIntervalMsProp, LONG, null, HIGH, LogFlushIntervalMsDoc)
      .define(LogFlushOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushOffsetCheckpointIntervalMsDoc)
      .define(LogFlushStartOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushStartOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushStartOffsetCheckpointIntervalMsDoc)
      .define(LogPreAllocateProp, BOOLEAN, Defaults.LogPreAllocateEnable, MEDIUM, LogPreAllocateEnableDoc)
      .define(NumRecoveryThreadsPerDataDirProp, INT, Defaults.NumRecoveryThreadsPerDataDir, atLeast(1), HIGH, NumRecoveryThreadsPerDataDirDoc)
      .define(AutoCreateTopicsEnableProp, BOOLEAN, Defaults.AutoCreateTopicsEnable, HIGH, AutoCreateTopicsEnableDoc)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), HIGH, MinInSyncReplicasDoc)
      .define(LogMessageFormatVersionProp, STRING, Defaults.LogMessageFormatVersion, new MetadataVersionValidator(), MEDIUM, LogMessageFormatVersionDoc)
      .define(LogMessageTimestampTypeProp, STRING, Defaults.LogMessageTimestampType, in("CreateTime", "LogAppendTime"), MEDIUM, LogMessageTimestampTypeDoc)
      .define(LogMessageTimestampDifferenceMaxMsProp, LONG, Defaults.LogMessageTimestampDifferenceMaxMs,  atLeast(0), MEDIUM, LogMessageTimestampDifferenceMaxMsDoc)
      .define(CreateTopicPolicyClassNameProp, CLASS, null, LOW, CreateTopicPolicyClassNameDoc)
      .define(AlterConfigPolicyClassNameProp, CLASS, null, LOW, AlterConfigPolicyClassNameDoc)
      .define(LogMessageDownConversionEnableProp, BOOLEAN, Defaults.MessageDownConversionEnable, LOW, LogMessageDownConversionEnableDoc)

      /** ********* Replication configuration ***********/
      .define(ControllerSocketTimeoutMsProp, INT, Defaults.ControllerSocketTimeoutMs, MEDIUM, ControllerSocketTimeoutMsDoc)
      .define(DefaultReplicationFactorProp, INT, Defaults.DefaultReplicationFactor, MEDIUM, DefaultReplicationFactorDoc)
      .define(ReplicaLagTimeMaxMsProp, LONG, Defaults.ReplicaLagTimeMaxMs, HIGH, ReplicaLagTimeMaxMsDoc)
      .define(ReplicaSocketTimeoutMsProp, INT, Defaults.ReplicaSocketTimeoutMs, HIGH, ReplicaSocketTimeoutMsDoc)
      .define(ReplicaSocketReceiveBufferBytesProp, INT, Defaults.ReplicaSocketReceiveBufferBytes, HIGH, ReplicaSocketReceiveBufferBytesDoc)
      .define(ReplicaFetchMaxBytesProp, INT, Defaults.ReplicaFetchMaxBytes, atLeast(0), MEDIUM, ReplicaFetchMaxBytesDoc)
      .define(ReplicaFetchWaitMaxMsProp, INT, Defaults.ReplicaFetchWaitMaxMs, HIGH, ReplicaFetchWaitMaxMsDoc)
      .define(ReplicaFetchBackoffMsProp, INT, Defaults.ReplicaFetchBackoffMs, atLeast(0), MEDIUM, ReplicaFetchBackoffMsDoc)
      .define(ReplicaFetchMinBytesProp, INT, Defaults.ReplicaFetchMinBytes, HIGH, ReplicaFetchMinBytesDoc)
      .define(ReplicaFetchResponseMaxBytesProp, INT, Defaults.ReplicaFetchResponseMaxBytes, atLeast(0), MEDIUM, ReplicaFetchResponseMaxBytesDoc)
      .define(NumReplicaFetchersProp, INT, Defaults.NumReplicaFetchers, HIGH, NumReplicaFetchersDoc)
      .define(ReplicaHighWatermarkCheckpointIntervalMsProp, LONG, Defaults.ReplicaHighWatermarkCheckpointIntervalMs, HIGH, ReplicaHighWatermarkCheckpointIntervalMsDoc)
      .define(FetchPurgatoryPurgeIntervalRequestsProp, INT, Defaults.FetchPurgatoryPurgeIntervalRequests, MEDIUM, FetchPurgatoryPurgeIntervalRequestsDoc)
      .define(ProducerPurgatoryPurgeIntervalRequestsProp, INT, Defaults.ProducerPurgatoryPurgeIntervalRequests, MEDIUM, ProducerPurgatoryPurgeIntervalRequestsDoc)
      .define(DeleteRecordsPurgatoryPurgeIntervalRequestsProp, INT, Defaults.DeleteRecordsPurgatoryPurgeIntervalRequests, MEDIUM, DeleteRecordsPurgatoryPurgeIntervalRequestsDoc)
      .define(AutoLeaderRebalanceEnableProp, BOOLEAN, Defaults.AutoLeaderRebalanceEnable, HIGH, AutoLeaderRebalanceEnableDoc)
      .define(LeaderImbalancePerBrokerPercentageProp, INT, Defaults.LeaderImbalancePerBrokerPercentage, HIGH, LeaderImbalancePerBrokerPercentageDoc)
      .define(LeaderImbalanceCheckIntervalSecondsProp, LONG, Defaults.LeaderImbalanceCheckIntervalSeconds, atLeast(1), HIGH, LeaderImbalanceCheckIntervalSecondsDoc)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable, HIGH, UncleanLeaderElectionEnableDoc)
      .define(InterBrokerSecurityProtocolProp, STRING, Defaults.InterBrokerSecurityProtocol, in(Utils.enumOptions(classOf[SecurityProtocol]):_*), MEDIUM, InterBrokerSecurityProtocolDoc)
      .define(InterBrokerProtocolVersionProp, STRING, Defaults.InterBrokerProtocolVersion, new MetadataVersionValidator(), MEDIUM, InterBrokerProtocolVersionDoc)
      .define(InterBrokerListenerNameProp, STRING, null, MEDIUM, InterBrokerListenerNameDoc)
      .define(ReplicaSelectorClassProp, STRING, null, MEDIUM, ReplicaSelectorClassDoc)

      /** ********* Controlled shutdown configuration ***********/
      .define(ControlledShutdownMaxRetriesProp, INT, Defaults.ControlledShutdownMaxRetries, MEDIUM, ControlledShutdownMaxRetriesDoc)
      .define(ControlledShutdownRetryBackoffMsProp, LONG, Defaults.ControlledShutdownRetryBackoffMs, MEDIUM, ControlledShutdownRetryBackoffMsDoc)
      .define(ControlledShutdownEnableProp, BOOLEAN, Defaults.ControlledShutdownEnable, MEDIUM, ControlledShutdownEnableDoc)

      /** ********* Group coordinator configuration ***********/
      .define(GroupMinSessionTimeoutMsProp, INT, Defaults.GroupMinSessionTimeoutMs, MEDIUM, GroupMinSessionTimeoutMsDoc)
      .define(GroupMaxSessionTimeoutMsProp, INT, Defaults.GroupMaxSessionTimeoutMs, MEDIUM, GroupMaxSessionTimeoutMsDoc)
      .define(GroupInitialRebalanceDelayMsProp, INT, Defaults.GroupInitialRebalanceDelayMs, MEDIUM, GroupInitialRebalanceDelayMsDoc)
      .define(GroupMaxSizeProp, INT, Defaults.GroupMaxSize, atLeast(1), MEDIUM, GroupMaxSizeDoc)

      /** ********* Offset management configuration ***********/
      .define(OffsetMetadataMaxSizeProp, INT, Defaults.OffsetMetadataMaxSize, HIGH, OffsetMetadataMaxSizeDoc)
      .define(OffsetsLoadBufferSizeProp, INT, Defaults.OffsetsLoadBufferSize, atLeast(1), HIGH, OffsetsLoadBufferSizeDoc)
      .define(OffsetsTopicReplicationFactorProp, SHORT, Defaults.OffsetsTopicReplicationFactor, atLeast(1), HIGH, OffsetsTopicReplicationFactorDoc)
      .define(OffsetsTopicPartitionsProp, INT, Defaults.OffsetsTopicPartitions, atLeast(1), HIGH, OffsetsTopicPartitionsDoc)
      .define(OffsetsTopicSegmentBytesProp, INT, Defaults.OffsetsTopicSegmentBytes, atLeast(1), HIGH, OffsetsTopicSegmentBytesDoc)
      .define(OffsetsTopicCompressionCodecProp, INT, Defaults.OffsetsTopicCompressionCodec, HIGH, OffsetsTopicCompressionCodecDoc)
      .define(OffsetsRetentionMinutesProp, INT, Defaults.OffsetsRetentionMinutes, atLeast(1), HIGH, OffsetsRetentionMinutesDoc)
      .define(OffsetsRetentionCheckIntervalMsProp, LONG, Defaults.OffsetsRetentionCheckIntervalMs, atLeast(1), HIGH, OffsetsRetentionCheckIntervalMsDoc)
      .define(OffsetCommitTimeoutMsProp, INT, Defaults.OffsetCommitTimeoutMs, atLeast(1), HIGH, OffsetCommitTimeoutMsDoc)
      .define(OffsetCommitRequiredAcksProp, SHORT, Defaults.OffsetCommitRequiredAcks, HIGH, OffsetCommitRequiredAcksDoc)
      .define(DeleteTopicEnableProp, BOOLEAN, Defaults.DeleteTopicEnable, HIGH, DeleteTopicEnableDoc)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(BrokerCompressionCodec.brokerCompressionOptions:_*), HIGH, CompressionTypeDoc)

      /** ********* Transaction management configuration ***********/
      .define(TransactionalIdExpirationMsProp, INT, Defaults.TransactionalIdExpirationMs, atLeast(1), HIGH, TransactionalIdExpirationMsDoc)
      .define(TransactionsMaxTimeoutMsProp, INT, Defaults.TransactionsMaxTimeoutMs, atLeast(1), HIGH, TransactionsMaxTimeoutMsDoc)
      .define(TransactionsTopicMinISRProp, INT, Defaults.TransactionsTopicMinISR, atLeast(1), HIGH, TransactionsTopicMinISRDoc)
      .define(TransactionsLoadBufferSizeProp, INT, Defaults.TransactionsLoadBufferSize, atLeast(1), HIGH, TransactionsLoadBufferSizeDoc)
      .define(TransactionsTopicReplicationFactorProp, SHORT, Defaults.TransactionsTopicReplicationFactor, atLeast(1), HIGH, TransactionsTopicReplicationFactorDoc)
      .define(TransactionsTopicPartitionsProp, INT, Defaults.TransactionsTopicPartitions, atLeast(1), HIGH, TransactionsTopicPartitionsDoc)
      .define(TransactionsTopicSegmentBytesProp, INT, Defaults.TransactionsTopicSegmentBytes, atLeast(1), HIGH, TransactionsTopicSegmentBytesDoc)
      .define(TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, INT, Defaults.TransactionsAbortTimedOutTransactionsCleanupIntervalMS, atLeast(1), LOW, TransactionsAbortTimedOutTransactionsIntervalMsDoc)
      .define(TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp, INT, Defaults.TransactionsRemoveExpiredTransactionsCleanupIntervalMS, atLeast(1), LOW, TransactionsRemoveExpiredTransactionsIntervalMsDoc)

      .define(ProducerIdExpirationMsProp, INT, Defaults.ProducerIdExpirationMs, atLeast(1), LOW, ProducerIdExpirationMsDoc)
      // Configuration for testing only as default value should be sufficient for typical usage
      .defineInternal(ProducerIdExpirationCheckIntervalMsProp, INT, Defaults.ProducerIdExpirationCheckIntervalMs, atLeast(1), LOW, ProducerIdExpirationCheckIntervalMsDoc)

      /** ********* Fetch Configuration **************/
      .define(MaxIncrementalFetchSessionCacheSlots, INT, Defaults.MaxIncrementalFetchSessionCacheSlots, atLeast(0), MEDIUM, MaxIncrementalFetchSessionCacheSlotsDoc)
      .define(FetchMaxBytes, INT, Defaults.FetchMaxBytes, atLeast(1024), MEDIUM, FetchMaxBytesDoc)

      /** ********* Kafka Metrics Configuration ***********/
      .define(MetricNumSamplesProp, INT, Defaults.MetricNumSamples, atLeast(1), LOW, MetricNumSamplesDoc)
      .define(MetricSampleWindowMsProp, LONG, Defaults.MetricSampleWindowMs, atLeast(1), LOW, MetricSampleWindowMsDoc)
      .define(MetricReporterClassesProp, LIST, Defaults.MetricReporterClasses, LOW, MetricReporterClassesDoc)
      .define(MetricRecordingLevelProp, STRING, Defaults.MetricRecordingLevel, LOW, MetricRecordingLevelDoc)
      .define(AutoIncludeJmxReporterProp, BOOLEAN, Defaults.AutoIncludeJmxReporter, LOW, AutoIncludeJmxReporterDoc)

      /** ********* Kafka Yammer Metrics Reporter Configuration for docs ***********/
      .define(KafkaMetricsReporterClassesProp, LIST, Defaults.KafkaMetricReporterClasses, LOW, KafkaMetricsReporterClassesDoc)
      .define(KafkaMetricsPollingIntervalSecondsProp, INT, Defaults.KafkaMetricsPollingIntervalSeconds, atLeast(1), LOW, KafkaMetricsPollingIntervalSecondsDoc)

      /** ********* Quota configuration ***********/
      .define(NumQuotaSamplesProp, INT, Defaults.NumQuotaSamples, atLeast(1), LOW, NumQuotaSamplesDoc)
      .define(NumReplicationQuotaSamplesProp, INT, Defaults.NumReplicationQuotaSamples, atLeast(1), LOW, NumReplicationQuotaSamplesDoc)
      .define(NumAlterLogDirsReplicationQuotaSamplesProp, INT, Defaults.NumAlterLogDirsReplicationQuotaSamples, atLeast(1), LOW, NumAlterLogDirsReplicationQuotaSamplesDoc)
      .define(NumControllerQuotaSamplesProp, INT, Defaults.NumControllerQuotaSamples, atLeast(1), LOW, NumControllerQuotaSamplesDoc)
      .define(QuotaWindowSizeSecondsProp, INT, Defaults.QuotaWindowSizeSeconds, atLeast(1), LOW, QuotaWindowSizeSecondsDoc)
      .define(ReplicationQuotaWindowSizeSecondsProp, INT, Defaults.ReplicationQuotaWindowSizeSeconds, atLeast(1), LOW, ReplicationQuotaWindowSizeSecondsDoc)
      .define(AlterLogDirsReplicationQuotaWindowSizeSecondsProp, INT, Defaults.AlterLogDirsReplicationQuotaWindowSizeSeconds, atLeast(1), LOW, AlterLogDirsReplicationQuotaWindowSizeSecondsDoc)
      .define(ControllerQuotaWindowSizeSecondsProp, INT, Defaults.ControllerQuotaWindowSizeSeconds, atLeast(1), LOW, ControllerQuotaWindowSizeSecondsDoc)
      .define(ClientQuotaCallbackClassProp, CLASS, null, LOW, ClientQuotaCallbackClassDoc)

      /** ********* General Security Configuration ****************/
      .define(ConnectionsMaxReauthMsProp, LONG, Defaults.ConnectionsMaxReauthMsDefault, MEDIUM, ConnectionsMaxReauthMsDoc)
      .define(SaslServerMaxReceiveSizeProp, INT, Defaults.DefaultServerMaxMaxReceiveSize, MEDIUM, SaslServerMaxReceiveSizeDoc)
      .define(securityProviderClassProp, STRING, null, LOW, securityProviderClassDoc)

      /** ********* SSL Configuration ****************/
      .define(PrincipalBuilderClassProp, CLASS, Defaults.DefaultPrincipalBuilder, MEDIUM, PrincipalBuilderClassDoc)
      .define(SslProtocolProp, STRING, Defaults.SslProtocol, MEDIUM, SslProtocolDoc)
      .define(SslProviderProp, STRING, null, MEDIUM, SslProviderDoc)
      .define(SslEnabledProtocolsProp, LIST, Defaults.SslEnabledProtocols, MEDIUM, SslEnabledProtocolsDoc)
      .define(SslKeystoreTypeProp, STRING, Defaults.SslKeystoreType, MEDIUM, SslKeystoreTypeDoc)
      .define(SslKeystoreLocationProp, STRING, null, MEDIUM, SslKeystoreLocationDoc)
      .define(SslKeystorePasswordProp, PASSWORD, null, MEDIUM, SslKeystorePasswordDoc)
      .define(SslKeyPasswordProp, PASSWORD, null, MEDIUM, SslKeyPasswordDoc)
      .define(SslKeystoreKeyProp, PASSWORD, null, MEDIUM, SslKeystoreKeyDoc)
      .define(SslKeystoreCertificateChainProp, PASSWORD, null, MEDIUM, SslKeystoreCertificateChainDoc)
      .define(SslTruststoreTypeProp, STRING, Defaults.SslTruststoreType, MEDIUM, SslTruststoreTypeDoc)
      .define(SslTruststoreLocationProp, STRING, null, MEDIUM, SslTruststoreLocationDoc)
      .define(SslTruststorePasswordProp, PASSWORD, null, MEDIUM, SslTruststorePasswordDoc)
      .define(SslTruststoreCertificatesProp, PASSWORD, null, MEDIUM, SslTruststoreCertificatesDoc)
      .define(SslKeyManagerAlgorithmProp, STRING, Defaults.SslKeyManagerAlgorithm, MEDIUM, SslKeyManagerAlgorithmDoc)
      .define(SslTrustManagerAlgorithmProp, STRING, Defaults.SslTrustManagerAlgorithm, MEDIUM, SslTrustManagerAlgorithmDoc)
      .define(SslEndpointIdentificationAlgorithmProp, STRING, Defaults.SslEndpointIdentificationAlgorithm, LOW, SslEndpointIdentificationAlgorithmDoc)
      .define(SslSecureRandomImplementationProp, STRING, null, LOW, SslSecureRandomImplementationDoc)
      .define(SslClientAuthProp, STRING, Defaults.SslClientAuthentication, in(Defaults.SslClientAuthenticationValidValues:_*), MEDIUM, SslClientAuthDoc)
      .define(SslCipherSuitesProp, LIST, Collections.emptyList(), MEDIUM, SslCipherSuitesDoc)
      .define(SslPrincipalMappingRulesProp, STRING, Defaults.SslPrincipalMappingRules, LOW, SslPrincipalMappingRulesDoc)
      .define(SslEngineFactoryClassProp, CLASS, null, LOW, SslEngineFactoryClassDoc)

      /** ********* Sasl Configuration ****************/
      .define(SaslMechanismInterBrokerProtocolProp, STRING, Defaults.SaslMechanismInterBrokerProtocol, MEDIUM, SaslMechanismInterBrokerProtocolDoc)
      .define(SaslJaasConfigProp, PASSWORD, null, MEDIUM, SaslJaasConfigDoc)
      .define(SaslEnabledMechanismsProp, LIST, Defaults.SaslEnabledMechanisms, MEDIUM, SaslEnabledMechanismsDoc)
      .define(SaslServerCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslServerCallbackHandlerClassDoc)
      .define(SaslClientCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslClientCallbackHandlerClassDoc)
      .define(SaslLoginClassProp, CLASS, null, MEDIUM, SaslLoginClassDoc)
      .define(SaslLoginCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslLoginCallbackHandlerClassDoc)
      .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, SaslKerberosServiceNameDoc)
      .define(SaslKerberosKinitCmdProp, STRING, Defaults.SaslKerberosKinitCmd, MEDIUM, SaslKerberosKinitCmdDoc)
      .define(SaslKerberosTicketRenewWindowFactorProp, DOUBLE, Defaults.SaslKerberosTicketRenewWindowFactor, MEDIUM, SaslKerberosTicketRenewWindowFactorDoc)
      .define(SaslKerberosTicketRenewJitterProp, DOUBLE, Defaults.SaslKerberosTicketRenewJitter, MEDIUM, SaslKerberosTicketRenewJitterDoc)
      .define(SaslKerberosMinTimeBeforeReloginProp, LONG, Defaults.SaslKerberosMinTimeBeforeRelogin, MEDIUM, SaslKerberosMinTimeBeforeReloginDoc)
      .define(SaslKerberosPrincipalToLocalRulesProp, LIST, Defaults.SaslKerberosPrincipalToLocalRules, MEDIUM, SaslKerberosPrincipalToLocalRulesDoc)
      .define(SaslLoginRefreshWindowFactorProp, DOUBLE, Defaults.SaslLoginRefreshWindowFactor, MEDIUM, SaslLoginRefreshWindowFactorDoc)
      .define(SaslLoginRefreshWindowJitterProp, DOUBLE, Defaults.SaslLoginRefreshWindowJitter, MEDIUM, SaslLoginRefreshWindowJitterDoc)
      .define(SaslLoginRefreshMinPeriodSecondsProp, SHORT, Defaults.SaslLoginRefreshMinPeriodSeconds, MEDIUM, SaslLoginRefreshMinPeriodSecondsDoc)
      .define(SaslLoginRefreshBufferSecondsProp, SHORT, Defaults.SaslLoginRefreshBufferSeconds, MEDIUM, SaslLoginRefreshBufferSecondsDoc)
      .define(SaslLoginConnectTimeoutMsProp, INT, null, LOW, SaslLoginConnectTimeoutMsDoc)
      .define(SaslLoginReadTimeoutMsProp, INT, null, LOW, SaslLoginReadTimeoutMsDoc)
      .define(SaslLoginRetryBackoffMaxMsProp, LONG, Defaults.SaslLoginRetryBackoffMaxMs, LOW, SaslLoginRetryBackoffMaxMsDoc)
      .define(SaslLoginRetryBackoffMsProp, LONG, Defaults.SaslLoginRetryBackoffMs, LOW, SaslLoginRetryBackoffMsDoc)
      .define(SaslOAuthBearerScopeClaimNameProp, STRING, Defaults.SaslOAuthBearerScopeClaimName, LOW, SaslOAuthBearerScopeClaimNameDoc)
      .define(SaslOAuthBearerSubClaimNameProp, STRING, Defaults.SaslOAuthBearerSubClaimName, LOW, SaslOAuthBearerSubClaimNameDoc)
      .define(SaslOAuthBearerTokenEndpointUrlProp, STRING, null, MEDIUM, SaslOAuthBearerTokenEndpointUrlDoc)
      .define(SaslOAuthBearerJwksEndpointUrlProp, STRING, null, MEDIUM, SaslOAuthBearerJwksEndpointUrlDoc)
      .define(SaslOAuthBearerJwksEndpointRefreshMsProp, LONG, Defaults.SaslOAuthBearerJwksEndpointRefreshMs, LOW, SaslOAuthBearerJwksEndpointRefreshMsDoc)
      .define(SaslOAuthBearerJwksEndpointRetryBackoffMsProp, LONG, Defaults.SaslOAuthBearerJwksEndpointRetryBackoffMs, LOW, SaslOAuthBearerJwksEndpointRetryBackoffMsDoc)
      .define(SaslOAuthBearerJwksEndpointRetryBackoffMaxMsProp, LONG, Defaults.SaslOAuthBearerJwksEndpointRetryBackoffMaxMs, LOW, SaslOAuthBearerJwksEndpointRetryBackoffMaxMsDoc)
      .define(SaslOAuthBearerClockSkewSecondsProp, INT, Defaults.SaslOAuthBearerClockSkewSeconds, LOW, SaslOAuthBearerClockSkewSecondsDoc)
      .define(SaslOAuthBearerExpectedAudienceProp, LIST, null, LOW, SaslOAuthBearerExpectedAudienceDoc)
      .define(SaslOAuthBearerExpectedIssuerProp, STRING, null, LOW, SaslOAuthBearerExpectedIssuerDoc)

      /** ********* Delegation Token Configuration ****************/
      .define(DelegationTokenSecretKeyAliasProp, PASSWORD, null, MEDIUM, DelegationTokenSecretKeyAliasDoc)
      .define(DelegationTokenSecretKeyProp, PASSWORD, null, MEDIUM, DelegationTokenSecretKeyDoc)
      .define(DelegationTokenMaxLifeTimeProp, LONG, Defaults.DelegationTokenMaxLifeTimeMsDefault, atLeast(1), MEDIUM, DelegationTokenMaxLifeTimeDoc)
      .define(DelegationTokenExpiryTimeMsProp, LONG, Defaults.DelegationTokenExpiryTimeMsDefault, atLeast(1), MEDIUM, DelegationTokenExpiryTimeMsDoc)
      .define(DelegationTokenExpiryCheckIntervalMsProp, LONG, Defaults.DelegationTokenExpiryCheckIntervalMsDefault, atLeast(1), LOW, DelegationTokenExpiryCheckIntervalDoc)

      /** ********* Password encryption configuration for dynamic configs *********/
      .define(PasswordEncoderSecretProp, PASSWORD, null, MEDIUM, PasswordEncoderSecretDoc)
      .define(PasswordEncoderOldSecretProp, PASSWORD, null, MEDIUM, PasswordEncoderOldSecretDoc)
      .define(PasswordEncoderKeyFactoryAlgorithmProp, STRING, null, LOW, PasswordEncoderKeyFactoryAlgorithmDoc)
      .define(PasswordEncoderCipherAlgorithmProp, STRING, Defaults.PasswordEncoderCipherAlgorithm, LOW, PasswordEncoderCipherAlgorithmDoc)
      .define(PasswordEncoderKeyLengthProp, INT, Defaults.PasswordEncoderKeyLength, atLeast(8), LOW, PasswordEncoderKeyLengthDoc)
      .define(PasswordEncoderIterationsProp, INT, Defaults.PasswordEncoderIterations, atLeast(1024), LOW, PasswordEncoderIterationsDoc)

      /** ********* Raft Quorum Configuration *********/
      .define(RaftConfig.QUORUM_VOTERS_CONFIG, LIST, Defaults.QuorumVoters, new RaftConfig.ControllerQuorumVotersValidator(), HIGH, RaftConfig.QUORUM_VOTERS_DOC)
      .define(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, Defaults.QuorumElectionTimeoutMs, null, HIGH, RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_DOC)
      .define(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, Defaults.QuorumFetchTimeoutMs, null, HIGH, RaftConfig.QUORUM_FETCH_TIMEOUT_MS_DOC)
      .define(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, Defaults.QuorumElectionBackoffMs, null, HIGH, RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
      .define(RaftConfig.QUORUM_LINGER_MS_CONFIG, INT, Defaults.QuorumLingerMs, null, MEDIUM, RaftConfig.QUORUM_LINGER_MS_DOC)
      .define(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, Defaults.QuorumRequestTimeoutMs, null, MEDIUM, RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_DOC)
      .define(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, Defaults.QuorumRetryBackoffMs, null, LOW, RaftConfig.QUORUM_RETRY_BACKOFF_MS_DOC)
  }

  /** ********* Remote Log Management Configuration *********/
  RemoteLogManagerConfig.CONFIG_DEF.configKeys().values().forEach(key => configDef.define(key))

  def configNames: Seq[String] = configDef.names.asScala.toBuffer.sorted
  private[server] def defaultValues: Map[String, _] = configDef.defaultValues.asScala
  private[server] def configKeys: Map[String, ConfigKey] = configDef.configKeys.asScala

  def fromProps(props: Properties): KafkaConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): KafkaConfig = {
    // mark 返回一个KafkaConfig
    new KafkaConfig(props, doLog)
  }

  def fromProps(defaults: Properties, overrides: Properties): KafkaConfig =
    fromProps(defaults, overrides, true)

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaConfig = {
    val props = new Properties()
    props ++= defaults
    props ++= overrides
    fromProps(props, doLog)
  }

  def apply(props: java.util.Map[_, _], doLog: Boolean = true): KafkaConfig = new KafkaConfig(props, doLog)

  private def typeOf(name: String): Option[ConfigDef.Type] = Option(configDef.configKeys.get(name)).map(_.`type`)

  def configType(configName: String): Option[ConfigDef.Type] = {
    val configType = configTypeExact(configName)
    if (configType.isDefined) {
      return configType
    }
    typeOf(configName) match {
      case Some(t) => Some(t)
      case None =>
        DynamicBrokerConfig.brokerConfigSynonyms(configName, matchListenerOverride = true).flatMap(typeOf).headOption
    }
  }

  private def configTypeExact(exactName: String): Option[ConfigDef.Type] = {
    val configType = typeOf(exactName).orNull
    if (configType != null) {
      Some(configType)
    } else {
      val configKey = DynamicConfig.Broker.brokerConfigDef.configKeys().get(exactName)
      if (configKey != null) {
        Some(configKey.`type`)
      } else {
        None
      }
    }
  }

  def maybeSensitive(configType: Option[ConfigDef.Type]): Boolean = {
    // If we can't determine the config entry type, treat it as a sensitive config to be safe
    configType.isEmpty || configType.contains(ConfigDef.Type.PASSWORD)
  }

  def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
    val maybeSensitive = resourceType match {
      case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
      case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name))
      case ConfigResource.Type.BROKER_LOGGER => false
      case _ => true
    }
    if (maybeSensitive) Password.HIDDEN else value
  }

  /**
   *
   * Copy a configuration map, populating some keys that we want to treat as synonyms.
   * 用于填充 broker.id 和 node.id 这两个同义词
   */
  def populateSynonyms(input: util.Map[_, _]): util.Map[Any, Any] = {
    val output = new util.HashMap[Any, Any](input)
    val brokerId = output.get(KafkaConfig.BrokerIdProp) // broker.id
    val nodeId = output.get(KafkaConfig.NodeIdProp) // node.id
    // mark broker.id 和 node.id 实际上是一样的而且必须要保持一致
    if (brokerId == null && nodeId != null) {
      output.put(KafkaConfig.BrokerIdProp, nodeId)
    } else if (brokerId != null && nodeId == null) {
      output.put(KafkaConfig.NodeIdProp, brokerId)
    }
    output
  }
}

class KafkaConfig private(doLog: Boolean, val props: java.util.Map[_, _], dynamicConfigOverride: Option[DynamicBrokerConfig])
  extends AbstractConfig(KafkaConfig.configDef, props, doLog) with Logging {

  def this(props: java.util.Map[_, _]) = this(true, KafkaConfig.populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, KafkaConfig.populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean, dynamicConfigOverride: Option[DynamicBrokerConfig]) =
    this(doLog, KafkaConfig.populateSynonyms(props), dynamicConfigOverride)

  // Cache the current config to avoid acquiring read lock to access from dynamicConfig
  @volatile private var currentConfig = this

  // mark 解析配置中当前broker的角色
  val processRoles: Set[ProcessRole] = parseProcessRoles()
  // mark 生成动态配置实例
  private[server] val dynamicConfig = dynamicConfigOverride.getOrElse(new DynamicBrokerConfig(this))

  private[server] def updateCurrentConfig(newConfig: KafkaConfig): Unit = {
    this.currentConfig = newConfig
  }

  // The following captures any system properties impacting ZooKeeper TLS configuration
  // and defines the default values this instance will use if no explicit config is given.
  // We make it part of each instance rather than the object to facilitate testing.
  // mark 这里对ZooKeeper TLS配置进行解析，并在配置缺省时给出对应的默认值(从环境变量获取)
  // mark 创建了一个zookeeper配置对象 直接调用的原生API 这个里面会加载一些系统配置以及JVM相关配置
  private val zkClientConfigViaSystemProperties = new ZKClientConfig()

  // mark kafka配置不会更改底层原始的配置 如果不一样的话默认使用当前类的配置
  override def originals: util.Map[String, AnyRef] =
    if (this eq currentConfig) super.originals else currentConfig.originals
  override def values: util.Map[String, _] =
    if (this eq currentConfig) super.values else currentConfig.values
  override def nonInternalValues: util.Map[String, _] =
    if (this eq currentConfig) super.nonInternalValues else currentConfig.values
  override def originalsStrings: util.Map[String, String] =
    if (this eq currentConfig) super.originalsStrings else currentConfig.originalsStrings
  override def originalsWithPrefix(prefix: String): util.Map[String, AnyRef] =
    if (this eq currentConfig) super.originalsWithPrefix(prefix) else currentConfig.originalsWithPrefix(prefix)
  override def valuesWithPrefixOverride(prefix: String): util.Map[String, AnyRef] =
    if (this eq currentConfig) super.valuesWithPrefixOverride(prefix) else currentConfig.valuesWithPrefixOverride(prefix)
  override def get(key: String): AnyRef =
    if (this eq currentConfig) super.get(key) else currentConfig.get(key)

  //  During dynamic update, we use the values from this config, these are only used in DynamicBrokerConfig
  private[server] def originalsFromThisConfig: util.Map[String, AnyRef] = super.originals
  private[server] def valuesFromThisConfig: util.Map[String, _] = super.values
  def valuesFromThisConfigWithPrefixOverride(prefix: String): util.Map[String, AnyRef] =
    super.valuesWithPrefixOverride(prefix)

  /** ********* Zookeeper Configuration ***********/
  // mark zookeeper的连接地址 zookeeper.connect
  val zkConnect: String = getString(KafkaConfig.ZkConnectProp)
  // mark zookeeper的会话超时时间 zookeeper.session.timeout.ms
  val zkSessionTimeoutMs: Int = getInt(KafkaConfig.ZkSessionTimeoutMsProp)
  // mark zookeeper的连接超时时间 zookeeper.connection.timeout.ms
  val zkConnectionTimeoutMs: Int =
    Option(getInt(KafkaConfig.ZkConnectionTimeoutMsProp)).map(_.toInt).getOrElse(getInt(KafkaConfig.ZkSessionTimeoutMsProp))

  // mark zookeeper是否开启安全访问控制 zookeeper.enable.secure.acls
  val zkEnableSecureAcls: Boolean = getBoolean(KafkaConfig.ZkEnableSecureAclsProp)
  // mark zookeeper的最大请求数
  val zkMaxInFlightRequests: Int = getInt(KafkaConfig.ZkMaxInFlightRequestsProp)

  // mark 远程日志系统相关配置 kafka提供了将日志存储到其他远程系统比如S3，数据库 的能力可以通过这里配置
  private val _remoteLogManagerConfig = new RemoteLogManagerConfig(this)
  def remoteLogManagerConfig = _remoteLogManagerConfig

  private def zkBooleanConfigOrSystemPropertyWithDefaultValue(propKey: String): Boolean = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false (String) to true/false (Boolean)
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getBoolean(propKey) else {
      val sysPropValue = KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
      sysPropValue match {
        case Some("true") => true
        case Some(_) => false
        case _ => getBoolean(propKey) // not specified so use the default value
      }
    }
  }

  private def zkStringConfigOrSystemPropertyWithDefaultValue(propKey: String): String = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getString(propKey) else {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey) match {
        case Some(v) => v
        case _ => getString(propKey) // not specified so use the default value
      }
    }
  }

  private def zkOptionalStringConfigOrSystemProperty(propKey: String): Option[String] = {
    Option(getString(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
    }
  }

  private def zkPasswordConfigOrSystemProperty(propKey: String): Option[Password] = {
    Option(getPassword(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map(new Password(_))
    }
  }

  private def zkListConfigOrSystemProperty(propKey: String): Option[util.List[String]] = {
    Option(getList(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map { sysProp =>
        sysProp.split("\\s*,\\s*").toBuffer.asJava
      }
    }
  }

  // mark zookeeper ssl连接相关配置
  val zkSslClientEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslClientEnableProp)
  // mark 以下的配置都是与zk TLS加密相关的配置，都用Option进行了包装 防止空指针的问题
  val zkClientCnxnSocketClassName = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkClientCnxnSocketProp)
  val zkSslKeyStoreLocation = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslKeyStoreLocationProp)
  val zkSslKeyStorePassword = zkPasswordConfigOrSystemProperty(KafkaConfig.ZkSslKeyStorePasswordProp)
  val zkSslKeyStoreType = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslKeyStoreTypeProp)
  val zkSslTrustStoreLocation = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslTrustStoreLocationProp)
  val zkSslTrustStorePassword = zkPasswordConfigOrSystemProperty(KafkaConfig.ZkSslTrustStorePasswordProp)
  val zkSslTrustStoreType = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslTrustStoreTypeProp)
  val ZkSslProtocol = zkStringConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslProtocolProp)
  val ZkSslEnabledProtocols = zkListConfigOrSystemProperty(KafkaConfig.ZkSslEnabledProtocolsProp)
  val ZkSslCipherSuites = zkListConfigOrSystemProperty(KafkaConfig.ZkSslCipherSuitesProp)
  val ZkSslEndpointIdentificationAlgorithm = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false to HTTPS/<blank>
    val kafkaProp = KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp
    val actuallyProvided = originals.containsKey(kafkaProp)
    if (actuallyProvided)
      getString(kafkaProp)
    else {

      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, kafkaProp) match {
        case Some("true") => "HTTPS"
        case Some(_) => ""
        case None => getString(kafkaProp) // not specified so use the default value
      }
    }
  }
  val ZkSslCrlEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslCrlEnableProp)
  val ZkSslOcspEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslOcspEnableProp)
  /** ********* General Configuration ***********/
  // mark 以下是kafka集群相关的配置 包括zookeeper集群模式以及最新的kraft集群模式

  // mark broker.id.generation.enable是否开启自动生成broker id 默认不开启
  val brokerIdGenerationEnable: Boolean = getBoolean(KafkaConfig.BrokerIdGenerationEnableProp)
  // mark reserved.broker.max.id 指定 Kafka 集群中自动分配的 broker ID 的最大值。
  val maxReservedBrokerId: Int = getInt(KafkaConfig.MaxReservedBrokerIdProp)
  // mark 获取briker.id
  var brokerId: Int = getInt(KafkaConfig.BrokerIdProp)
  // mark 获取node.id
  val nodeId: Int = getInt(KafkaConfig.NodeIdProp)
  // mark initial.broker.registration.timeout.ms 它用于控制在 Kafka 集群启动时，Kafka Broker 等待注册成功的超时时间。
  val initialRegistrationTimeoutMs: Int = getInt(KafkaConfig.InitialBrokerRegistrationTimeoutMsProp)
  // mark broker.heartbeat.interval.ms 向kafka controller发送心跳的时间间隔
  val brokerHeartbeatIntervalMs: Int = getInt(KafkaConfig.BrokerHeartbeatIntervalMsProp)
  // mark broker.session.timeout.ms用于控制Broker与Controller的session会话超时时间
  val brokerSessionTimeoutMs: Int = getInt(KafkaConfig.BrokerSessionTimeoutMsProp)

  // mark process roles 是给raft使用的 zookeeper是不需要角色的
  def requiresZookeeper: Boolean = processRoles.isEmpty

  // mark 如果角色集合不为空则启动自我管理仲裁机制（不依赖zookeeper采用Kraft协议）
  def usesSelfManagedQuorum: Boolean = processRoles.nonEmpty

  // mark 是否启用 Kafka 从 ZooKeeper --> KRaft 迁移的功能。
  val migrationEnabled: Boolean = getBoolean(KafkaConfig.MigrationEnabledProp)

  /**
   * mark 在kraft模式中解析kafka broker的角色
   * 解析配置中指定的process.roles属性，并返回一个去重后的角色集合。
   * 这个方法首先将配置中的process.roles属性转换为Scala的List，然后映射到对应的ProcessRole枚举值。
   * 如果遇到未知的角色名称，将抛出ConfigException异常。
   * 如果配置中存在重复的角色名称，同样会抛出ConfigException异常。
   *
   * @return Set[ProcessRole] 返回一个不包含重复角色的集合。
   */
  private def parseProcessRoles(): Set[ProcessRole] = {
    // mark 解析process.roles配置，并映射到ProcessRole枚举
    val roles = getList(KafkaConfig.ProcessRolesProp).asScala.map {
      case "broker" => BrokerRole
      case "controller" => ControllerRole
      case role => throw new ConfigException(s"Unknown process role '$role'" +
        " (only 'broker' and 'controller' are allowed roles)")
    }

    // mark 将角色列表转换为集合以去除重复项
    val distinctRoles: Set[ProcessRole] = roles.toSet

    // mark 如果存在重复角色，则抛出异常
    if (distinctRoles.size != roles.size) {
      throw new ConfigException(s"Duplicate role names found in `${KafkaConfig.ProcessRolesProp}`: $roles")
    }

    distinctRoles
  }


  /**
   * 判断当前进程是否处于KRaft协调居民模式。
   *
   * 在KRaft模式中，Broker和Controller角色可能共享同一个进程。此方法检查当前进程是否同时担任了Broker和Controller角色，
   * 以此来确定是否处于KRaft协调居民模式。
   *
   * @return 如果当前进程同时担任Broker和Controller角色，则返回true；否则返回false。
   */
  def isKRaftCoResidentMode: Boolean = {
    processRoles == Set(BrokerRole, ControllerRole)
  }


  /**
   * 获取元数据日志目录。 如果启用非zookeeper模式 需要将元数据进行落盘
   *
   * 本方法旨在确定元数据日志的存储位置。它首先尝试从配置中获取指定的元数据日志目录。
   * 如果配置中未明确指定该目录，则默认使用可用的日志目录中的第一个。
   *
   * @return 元数据日志目录的字符串表示。
   */
  def metadataLogDir: String = {
    // 尝试从配置中获取元数据日志目录
    Option(getString(KafkaConfig.MetadataLogDirProp)) match {
      case Some(dir) => dir // 如果配置存在，则返回该目录
      case None => logDirs.head // 如果配置不存在，则返回默认的日志目录
    }
  }

  def metadataLogSegmentBytes = getInt(KafkaConfig.MetadataLogSegmentBytesProp)
  def metadataLogSegmentMillis = getLong(KafkaConfig.MetadataLogSegmentMillisProp)
  def metadataRetentionBytes = getLong(KafkaConfig.MetadataMaxRetentionBytesProp)
  def metadataRetentionMillis = getLong(KafkaConfig.MetadataMaxRetentionMillisProp)

  def numNetworkThreads = getInt(KafkaConfig.NumNetworkThreadsProp)

  // mark 获取后台线程的数量
  def backgroundThreads = getInt(KafkaConfig.BackgroundThreadsProp)

  // mark queued.max.requests限制 Kafka Broker 在处理客户端请求时，可以在队列中积压的最大请求数。
  val queuedMaxRequests = getInt(KafkaConfig.QueuedMaxRequestsProp)
  // mark queued.max.requests限制 Kafka Broker 在处理客户端请求时，可以在队列中积压的最大字节数。
  val queuedMaxBytes = getLong(KafkaConfig.QueuedMaxBytesProp)
  def numIoThreads = getInt(KafkaConfig.NumIoThreadsProp)
  def messageMaxBytes = getInt(KafkaConfig.MessageMaxBytesProp)

  // mark request.timeout.ms 配置项用于控制 Broker 处理请求的超时时间。它定义了 Broker 在处理客户端请求时，等待请求完成的最长时间
  val requestTimeoutMs = getInt(KafkaConfig.RequestTimeoutMsProp)
  // mark socket.connection.setup.timeout.ms设置 Kafka Broker 在与客户端建立网络连接时，等待连接成功的最大时间。
  val connectionSetupTimeoutMs = getLong(KafkaConfig.ConnectionSetupTimeoutMsProp)
  // mark socket.connection.setup.timeout.max.ms 设置 Kafka Broker 在与客户端建立网络连接时，等待连接成功的最大时间。
  val connectionSetupTimeoutMaxMs = getLong(KafkaConfig.ConnectionSetupTimeoutMaxMsProp)

  def getNumReplicaAlterLogDirsThreads: Int = {
    val numThreads: Integer = Option(getInt(KafkaConfig.NumReplicaAlterLogDirsThreadsProp)).getOrElse(logDirs.size)
    numThreads
  }

  /************* Metadata Configuration ***********/
  /**
   * Kafka 集群中的元数据日志包含了集群配置信息、主题、分区、分配等关键数据。为了确保这些元数据在崩溃或重启时能够快速恢复，
   * Kafka 定期创建快照，将当前的元数据状态保存到一个快照文件中。以下是与元数据相关的一些配置项
   */
  // mark metadata.log.max.record.bytes.between.snapshots 指定在创建新的元数据日志快照之前，Kafka 元数据日志中允许的最大记录字节数。
  val metadataSnapshotMaxNewRecordBytes = getLong(KafkaConfig.MetadataSnapshotMaxNewRecordBytesProp)
  // mark metadata.log.max.snapshot.interval.ms 指定 Kafka 创建新的元数据日志快照的最大时间间隔。
  val metadataSnapshotMaxIntervalMs = getLong(KafkaConfig.MetadataSnapshotMaxIntervalMsProp)
  // mark metadata.max.idle.interval.ms 
  val metadataMaxIdleIntervalNs: Option[Long] = {
    val value = TimeUnit.NANOSECONDS.convert(getInt(KafkaConfig.MetadataMaxIdleIntervalMsProp).toLong, TimeUnit.MILLISECONDS)
    if (value > 0) Some(value) else None
  }

  /************* Authorizer Configuration ***********/
  def createNewAuthorizer(): Option[Authorizer] = {
    val className = getString(KafkaConfig.AuthorizerClassNameProp)
    if (className == null || className.isEmpty)
      None
    else {
      Some(AuthorizerUtils.createAuthorizer(className))
    }
  }

  /**
   * mark 提取并验证早期启动监听器的名称。(kraft在启动初期就可以提供服务)
   *
   * 早期启动监听器是指在Kafka服务器启动过程中需要提前启动的一组监听器。这个值是从配置中获取的，
   * 并且需要与`listeners`和`controllerListeners`配置中的监听器名称匹配。如果配置中指定了早期启动监听器，
   * 但它们并未在其他监听器配置中定义，那么将抛出配置异常。
   *
   * @return 一个集合，包含所有指定为早期启动的监听器名称。
   */
  val earlyStartListeners: Set[ListenerName] = {
    // 提取并转换`listeners`配置中的监听器名称，形成一个集合
    // mark 获取监听器名称
    val listenersSet = listeners.map(_.listenerName).toSet
    // 提取并转换`controllerListeners`配置中的监听器名称，形成一个集合
    // mark kraft模式下的控制器监听器名称
    val controllerListenersSet = controllerListeners.map(_.listenerName).toSet

    // 根据配置中是否指定了早期启动监听器，采取不同的处理策略
    // mark 早起监听器名称
    Option(getString(KafkaConfig.EarlyStartListenersProp)) match {
      // 如果没有指定早期启动监听器，则认为所有控制器监听器都是早期启动监听器
      case None => controllerListenersSet
      // 如果指定了早期启动监听器，则进行进一步的验证和处理
      case Some(str) =>
        // 分割字符串，去除空格，并过滤空字符串，形成早期启动监听器名称的集合
        str.split(",").map(_.trim()).filterNot(_.isEmpty).map { str =>
          // 创建一个监听器名称对象
          val listenerName = new ListenerName(str)
          // 验证该监听器名称是否在其他监听器配置中存在，如果不存在，则抛出配置异常
          if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName))
            throw new ConfigException(s"${KafkaConfig.EarlyStartListenersProp} contains " +
              s"listener ${listenerName.value()}, but this is not contained in " +
              s"${KafkaConfig.ListenersProp} or ${KafkaConfig.ControllerListenerNamesProp}")
          // 如果验证通过，将监听器名称添加到结果集合中
          listenerName
        }.toSet
    }
  }


  /** ********* Socket Server Configuration ***********/
  val socketSendBufferBytes = getInt(KafkaConfig.SocketSendBufferBytesProp)
  val socketReceiveBufferBytes = getInt(KafkaConfig.SocketReceiveBufferBytesProp)
  val socketRequestMaxBytes = getInt(KafkaConfig.SocketRequestMaxBytesProp)
  val socketListenBacklogSize = getInt(KafkaConfig.SocketListenBacklogSizeProp)
  val maxConnectionsPerIp = getInt(KafkaConfig.MaxConnectionsPerIpProp)
  val maxConnectionsPerIpOverrides: Map[String, Int] =
    getMap(KafkaConfig.MaxConnectionsPerIpOverridesProp, getString(KafkaConfig.MaxConnectionsPerIpOverridesProp)).map { case (k, v) => (k, v.toInt)}
  def maxConnections = getInt(KafkaConfig.MaxConnectionsProp)
  def maxConnectionCreationRate = getInt(KafkaConfig.MaxConnectionCreationRateProp)
  val connectionsMaxIdleMs = getLong(KafkaConfig.ConnectionsMaxIdleMsProp)
  val failedAuthenticationDelayMs = getInt(KafkaConfig.FailedAuthenticationDelayMsProp)

  /***************** rack configuration **************/
  val rack = Option(getString(KafkaConfig.RackProp))
  val replicaSelectorClassName = Option(getString(KafkaConfig.ReplicaSelectorClassProp))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(KafkaConfig.AutoCreateTopicsEnableProp)
  val numPartitions = getInt(KafkaConfig.NumPartitionsProp)
  /** log.dirs:卡夫卡的日志目录，默认为/tmp/kafka-logs */
  val logDirs = CoreUtils.parseCsvList(Option(getString(KafkaConfig.LogDirsProp)).getOrElse(getString(KafkaConfig.LogDirProp)))

  def logSegmentBytes = getInt(KafkaConfig.LogSegmentBytesProp)
  def logFlushIntervalMessages = getLong(KafkaConfig.LogFlushIntervalMessagesProp)
  val logCleanerThreads = getInt(KafkaConfig.LogCleanerThreadsProp)
  def numRecoveryThreadsPerDataDir = getInt(KafkaConfig.NumRecoveryThreadsPerDataDirProp)
  val logFlushSchedulerIntervalMs = getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp)
  val logFlushOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushOffsetCheckpointIntervalMsProp).toLong
  val logFlushStartOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushStartOffsetCheckpointIntervalMsProp).toLong
  val logCleanupIntervalMs = getLong(KafkaConfig.LogCleanupIntervalMsProp)
  def logCleanupPolicy = getList(KafkaConfig.LogCleanupPolicyProp)
  val offsetsRetentionMinutes = getInt(KafkaConfig.OffsetsRetentionMinutesProp)
  val offsetsRetentionCheckIntervalMs = getLong(KafkaConfig.OffsetsRetentionCheckIntervalMsProp)
  def logRetentionBytes = getLong(KafkaConfig.LogRetentionBytesProp)
  val logCleanerDedupeBufferSize = getLong(KafkaConfig.LogCleanerDedupeBufferSizeProp)
  val logCleanerDedupeBufferLoadFactor = getDouble(KafkaConfig.LogCleanerDedupeBufferLoadFactorProp)
  val logCleanerIoBufferSize = getInt(KafkaConfig.LogCleanerIoBufferSizeProp)
  val logCleanerIoMaxBytesPerSecond = getDouble(KafkaConfig.LogCleanerIoMaxBytesPerSecondProp)
  def logCleanerDeleteRetentionMs = getLong(KafkaConfig.LogCleanerDeleteRetentionMsProp)
  def logCleanerMinCompactionLagMs = getLong(KafkaConfig.LogCleanerMinCompactionLagMsProp)
  def logCleanerMaxCompactionLagMs = getLong(KafkaConfig.LogCleanerMaxCompactionLagMsProp)
  val logCleanerBackoffMs = getLong(KafkaConfig.LogCleanerBackoffMsProp)
  def logCleanerMinCleanRatio = getDouble(KafkaConfig.LogCleanerMinCleanRatioProp)
  val logCleanerEnable = getBoolean(KafkaConfig.LogCleanerEnableProp)
  def logIndexSizeMaxBytes = getInt(KafkaConfig.LogIndexSizeMaxBytesProp)
  def logIndexIntervalBytes = getInt(KafkaConfig.LogIndexIntervalBytesProp)
  def logDeleteDelayMs = getLong(KafkaConfig.LogDeleteDelayMsProp)
  def logRollTimeMillis: java.lang.Long = Option(getLong(KafkaConfig.LogRollTimeMillisProp)).getOrElse(60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeHoursProp))
  def logRollTimeJitterMillis: java.lang.Long = Option(getLong(KafkaConfig.LogRollTimeJitterMillisProp)).getOrElse(60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeJitterHoursProp))
  def logFlushIntervalMs: java.lang.Long = Option(getLong(KafkaConfig.LogFlushIntervalMsProp)).getOrElse(getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp))
  def minInSyncReplicas = getInt(KafkaConfig.MinInSyncReplicasProp)
  def logPreAllocateEnable: java.lang.Boolean = getBoolean(KafkaConfig.LogPreAllocateProp)

  // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  @nowarn("cat=deprecation")
  private val logMessageFormatVersionString = getString(KafkaConfig.LogMessageFormatVersionProp)

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  lazy val logMessageFormatVersion =
    if (LogConfig.shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion))
      MetadataVersion.fromVersionString(Defaults.LogMessageFormatVersion)
    else MetadataVersion.fromVersionString(logMessageFormatVersionString)

  def logMessageTimestampType = TimestampType.forName(getString(KafkaConfig.LogMessageTimestampTypeProp))
  def logMessageTimestampDifferenceMaxMs: Long = getLong(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)
  def logMessageDownConversionEnable: Boolean = getBoolean(KafkaConfig.LogMessageDownConversionEnableProp)

  /** ********* Replication configuration ***********/
  // mark 请求控制器的连接超时时间
  val controllerSocketTimeoutMs: Int = getInt(KafkaConfig.ControllerSocketTimeoutMsProp)
  val defaultReplicationFactor: Int = getInt(KafkaConfig.DefaultReplicationFactorProp)
  val replicaLagTimeMaxMs = getLong(KafkaConfig.ReplicaLagTimeMaxMsProp)
  val replicaSocketTimeoutMs = getInt(KafkaConfig.ReplicaSocketTimeoutMsProp)
  val replicaSocketReceiveBufferBytes = getInt(KafkaConfig.ReplicaSocketReceiveBufferBytesProp)
  val replicaFetchMaxBytes = getInt(KafkaConfig.ReplicaFetchMaxBytesProp)
  val replicaFetchWaitMaxMs = getInt(KafkaConfig.ReplicaFetchWaitMaxMsProp)
  val replicaFetchMinBytes = getInt(KafkaConfig.ReplicaFetchMinBytesProp)
  val replicaFetchResponseMaxBytes = getInt(KafkaConfig.ReplicaFetchResponseMaxBytesProp)
  val replicaFetchBackoffMs = getInt(KafkaConfig.ReplicaFetchBackoffMsProp)
  def numReplicaFetchers = getInt(KafkaConfig.NumReplicaFetchersProp)
  val replicaHighWatermarkCheckpointIntervalMs = getLong(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp)
  val fetchPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp)
  val producerPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp)
  val deleteRecordsPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.DeleteRecordsPurgatoryPurgeIntervalRequestsProp)
  val autoLeaderRebalanceEnable = getBoolean(KafkaConfig.AutoLeaderRebalanceEnableProp)
  val leaderImbalancePerBrokerPercentage = getInt(KafkaConfig.LeaderImbalancePerBrokerPercentageProp)
  val leaderImbalanceCheckIntervalSeconds: Long = getLong(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp)
  def uncleanLeaderElectionEnable: java.lang.Boolean = getBoolean(KafkaConfig.UncleanLeaderElectionEnableProp)

  // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  val interBrokerProtocolVersionString = getString(KafkaConfig.InterBrokerProtocolVersionProp)

  val interBrokerProtocolVersion = if (processRoles.isEmpty) {
    MetadataVersion.fromVersionString(interBrokerProtocolVersionString)
  } else {
    if (originals.containsKey(KafkaConfig.InterBrokerProtocolVersionProp)) {
      // A user-supplied IBP was given
      val configuredVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString)
      if (!configuredVersion.isKRaftSupported) {
        throw new ConfigException(s"A non-KRaft version ${interBrokerProtocolVersionString} given for ${KafkaConfig.InterBrokerProtocolVersionProp}. " +
          s"The minimum version is ${MetadataVersion.MINIMUM_KRAFT_VERSION}")
      } else {
        warn(s"${KafkaConfig.InterBrokerProtocolVersionProp} is deprecated in KRaft mode as of 3.3 and will only " +
          s"be read when first upgrading from a KRaft prior to 3.3. See kafka-storage.sh help for details on setting " +
          s"the metadata version for a new KRaft cluster.")
      }
    }
    // In KRaft mode, we pin this value to the minimum KRaft-supported version. This prevents inadvertent usage of
    // the static IBP config in broker components running in KRaft mode
    MetadataVersion.MINIMUM_KRAFT_VERSION
  }

  /** ********* Controlled shutdown configuration ***********/
  val controlledShutdownMaxRetries = getInt(KafkaConfig.ControlledShutdownMaxRetriesProp)
  val controlledShutdownRetryBackoffMs = getLong(KafkaConfig.ControlledShutdownRetryBackoffMsProp)
  val controlledShutdownEnable = getBoolean(KafkaConfig.ControlledShutdownEnableProp)

  /** ********* Feature configuration ***********/
  def isFeatureVersioningSupported = interBrokerProtocolVersion.isFeatureVersioningSupported()

  /** ********* Group coordinator configuration ***********/
  val groupMinSessionTimeoutMs = getInt(KafkaConfig.GroupMinSessionTimeoutMsProp)
  val groupMaxSessionTimeoutMs = getInt(KafkaConfig.GroupMaxSessionTimeoutMsProp)
  val groupInitialRebalanceDelay = getInt(KafkaConfig.GroupInitialRebalanceDelayMsProp)
  val groupMaxSize = getInt(KafkaConfig.GroupMaxSizeProp)

  /** ********* Offset management configuration ***********/
  val offsetMetadataMaxSize = getInt(KafkaConfig.OffsetMetadataMaxSizeProp)
  val offsetsLoadBufferSize = getInt(KafkaConfig.OffsetsLoadBufferSizeProp)
  val offsetsTopicReplicationFactor = getShort(KafkaConfig.OffsetsTopicReplicationFactorProp)
  val offsetsTopicPartitions = getInt(KafkaConfig.OffsetsTopicPartitionsProp)
  val offsetCommitTimeoutMs = getInt(KafkaConfig.OffsetCommitTimeoutMsProp)
  val offsetCommitRequiredAcks = getShort(KafkaConfig.OffsetCommitRequiredAcksProp)
  val offsetsTopicSegmentBytes = getInt(KafkaConfig.OffsetsTopicSegmentBytesProp)
  val offsetsTopicCompressionCodec = Option(getInt(KafkaConfig.OffsetsTopicCompressionCodecProp)).map(value => CompressionCodec.getCompressionCodec(value)).orNull

  /** ********* Transaction management configuration ***********/
  val transactionalIdExpirationMs = getInt(KafkaConfig.TransactionalIdExpirationMsProp)
  val transactionMaxTimeoutMs = getInt(KafkaConfig.TransactionsMaxTimeoutMsProp)
  val transactionTopicMinISR = getInt(KafkaConfig.TransactionsTopicMinISRProp)
  val transactionsLoadBufferSize = getInt(KafkaConfig.TransactionsLoadBufferSizeProp)
  val transactionTopicReplicationFactor = getShort(KafkaConfig.TransactionsTopicReplicationFactorProp)
  val transactionTopicPartitions = getInt(KafkaConfig.TransactionsTopicPartitionsProp)
  val transactionTopicSegmentBytes = getInt(KafkaConfig.TransactionsTopicSegmentBytesProp)
  val transactionAbortTimedOutTransactionCleanupIntervalMs = getInt(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp)
  val transactionRemoveExpiredTransactionalIdCleanupIntervalMs = getInt(KafkaConfig.TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp)

  val producerIdExpirationMs = getInt(KafkaConfig.ProducerIdExpirationMsProp)
  val producerIdExpirationCheckIntervalMs = getInt(KafkaConfig.ProducerIdExpirationCheckIntervalMsProp)

  /** ********* Metric Configuration **************/
  val metricNumSamples = getInt(KafkaConfig.MetricNumSamplesProp)
  val metricSampleWindowMs = getLong(KafkaConfig.MetricSampleWindowMsProp)
  val metricRecordingLevel = getString(KafkaConfig.MetricRecordingLevelProp)

  /** ********* SSL/SASL Configuration **************/
  // Security configs may be overridden for listeners, so it is not safe to use the base values
  // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
  // retrieved using KafkaConfig#valuesWithPrefixOverride
  private def saslEnabledMechanisms(listenerName: ListenerName): Set[String] = {
    val value = valuesWithPrefixOverride(listenerName.configPrefix).get(KafkaConfig.SaslEnabledMechanismsProp)
    if (value != null)
      value.asInstanceOf[util.List[String]].asScala.toSet
    else
      Set.empty[String]
  }

  // mark 返回集群内部通信监听器的名称，通过访问预定义的元组的第一项实现
  def interBrokerListenerName: ListenerName = getInterBrokerListenerNameAndSecurityProtocol._1

  // mark 返回集群内部通信的安全协议，通过访问预定义的元组的第二项实现
  def interBrokerSecurityProtocol: SecurityProtocol = getInterBrokerListenerNameAndSecurityProtocol._2

  // mark 返回控制平面监听器的名称，通过对包含监听器名称和安全协议的元组进行映射操作，仅保留监听器名称
  def controlPlaneListenerName: Option[ListenerName] = getControlPlaneListenerNameAndSecurityProtocol.map { case (listenerName, _) => listenerName }

  // mark 返回控制平面使用的安全协议，通过对包含监听器名称和安全协议的元组进行映射操作，仅保留安全协议
  def controlPlaneSecurityProtocol: Option[SecurityProtocol] = getControlPlaneListenerNameAndSecurityProtocol.map { case (_, securityProtocol) => securityProtocol }

  // mark 集群内部如果使用sasl通信需要指定通信机制
  def saslMechanismInterBrokerProtocol: String = getString(KafkaConfig.SaslMechanismInterBrokerProtocolProp)

  val saslInterBrokerHandshakeRequestEnable: Boolean = interBrokerProtocolVersion.isSaslInterBrokerHandshakeRequestEnabled()

  /** ********* DelegationToken Configuration **************/
  val delegationTokenSecretKey: Password = Option(getPassword(KafkaConfig.DelegationTokenSecretKeyProp))
    .getOrElse(getPassword(KafkaConfig.DelegationTokenSecretKeyAliasProp))
  val tokenAuthEnabled = (delegationTokenSecretKey != null && !delegationTokenSecretKey.value.isEmpty)
  val delegationTokenMaxLifeMs = getLong(KafkaConfig.DelegationTokenMaxLifeTimeProp)
  val delegationTokenExpiryTimeMs = getLong(KafkaConfig.DelegationTokenExpiryTimeMsProp)
  val delegationTokenExpiryCheckIntervalMs = getLong(KafkaConfig.DelegationTokenExpiryCheckIntervalMsProp)

  /** ********* Password encryption configuration for dynamic configs *********/
  def passwordEncoderSecret = Option(getPassword(KafkaConfig.PasswordEncoderSecretProp))
  def passwordEncoderOldSecret = Option(getPassword(KafkaConfig.PasswordEncoderOldSecretProp))
  def passwordEncoderCipherAlgorithm = getString(KafkaConfig.PasswordEncoderCipherAlgorithmProp)
  def passwordEncoderKeyFactoryAlgorithm = Option(getString(KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp))
  def passwordEncoderKeyLength = getInt(KafkaConfig.PasswordEncoderKeyLengthProp)
  def passwordEncoderIterations = getInt(KafkaConfig.PasswordEncoderIterationsProp)

  /** ********* Quota Configuration **************/
  val numQuotaSamples = getInt(KafkaConfig.NumQuotaSamplesProp)
  val quotaWindowSizeSeconds = getInt(KafkaConfig.QuotaWindowSizeSecondsProp)
  val numReplicationQuotaSamples = getInt(KafkaConfig.NumReplicationQuotaSamplesProp)
  val replicationQuotaWindowSizeSeconds = getInt(KafkaConfig.ReplicationQuotaWindowSizeSecondsProp)
  val numAlterLogDirsReplicationQuotaSamples = getInt(KafkaConfig.NumAlterLogDirsReplicationQuotaSamplesProp)
  val alterLogDirsReplicationQuotaWindowSizeSeconds = getInt(KafkaConfig.AlterLogDirsReplicationQuotaWindowSizeSecondsProp)
  val numControllerQuotaSamples = getInt(KafkaConfig.NumControllerQuotaSamplesProp)
  val controllerQuotaWindowSizeSeconds = getInt(KafkaConfig.ControllerQuotaWindowSizeSecondsProp)

  /** ********* Fetch Configuration **************/
  val maxIncrementalFetchSessionCacheSlots = getInt(KafkaConfig.MaxIncrementalFetchSessionCacheSlots)
  val fetchMaxBytes = getInt(KafkaConfig.FetchMaxBytes)

  val deleteTopicEnable = getBoolean(KafkaConfig.DeleteTopicEnableProp)
  def compressionType = getString(KafkaConfig.CompressionTypeProp)

  /** ********* Raft Quorum Configuration *********/
  val quorumVoters = getList(RaftConfig.QUORUM_VOTERS_CONFIG)
  val quorumElectionTimeoutMs = getInt(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG)
  val quorumFetchTimeoutMs = getInt(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG)
  val quorumElectionBackoffMs = getInt(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG)
  val quorumLingerMs = getInt(RaftConfig.QUORUM_LINGER_MS_CONFIG)
  val quorumRequestTimeoutMs = getInt(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG)
  val quorumRetryBackoffMs = getInt(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG)

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.addReconfigurable(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.removeReconfigurable(reconfigurable)
  }

  def logRetentionTimeMillis: Long = {
    val millisInMinute = 60L * 1000L
    val millisInHour = 60L * millisInMinute

    val millis: java.lang.Long =
      Option(getLong(KafkaConfig.LogRetentionTimeMillisProp)).getOrElse(
        Option(getInt(KafkaConfig.LogRetentionTimeMinutesProp)) match {
          case Some(mins) => millisInMinute * mins
          case None => getInt(KafkaConfig.LogRetentionTimeHoursProp) * millisInHour
        })

    if (millis < 0) return -1
    millis
  }

  private def getMap(propName: String, propValue: String): Map[String, String] = {
    try {
      CoreUtils.parseCsvMap(propValue)
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(propName, e.getMessage))
    }
  }

  /**
   * 获取监听器列表。
   *
   * 本方法通过解析配置获取Kafka服务器的监听器序列。监听器定义了Kafka服务器如何接收来自客户端的连接和请求。
   * 它们是Kafka提供服务的关键组件，允许多种类型的客户端以不同的协议和安全性连接到服务器。
   *
   * @return Seq[EndPoint] - 返回一个序列，其中包含所有监听器的端点信息。每个监听器端点代表一个服务器接收连接的网络地址和端口。
   */
  def listeners: Seq[EndPoint] =
    CoreUtils.listenerListToEndPoints(getString(KafkaConfig.ListenersProp), effectiveListenerSecurityProtocolMap)

  def controllerListenerNames: Seq[String] = {
    val value = Option(getString(KafkaConfig.ControllerListenerNamesProp)).getOrElse("")
    if (value.isEmpty) {
      Seq.empty
    } else {
      value.split(",")
    }
  }

  def controllerListeners: Seq[EndPoint] =
    listeners.filter(l => controllerListenerNames.contains(l.listenerName.value()))

  def saslMechanismControllerProtocol: String = getString(KafkaConfig.SaslMechanismControllerProtocolProp)

  def controlPlaneListener: Option[EndPoint] = {
    controlPlaneListenerName.map { listenerName =>
      listeners.filter(endpoint => endpoint.listenerName.value() == listenerName.value()).head
    }
  }

  def dataPlaneListeners: Seq[EndPoint] = {
    listeners.filterNot { listener =>
      val name = listener.listenerName.value()
      name.equals(getString(KafkaConfig.ControlPlaneListenerNameProp)) ||
        controllerListenerNames.contains(name)
    }
  }

  /**
   * 获取有效的外部监听器Endpoint集合。
   *
   * 此方法用于确定Kafka服务器将向客户端宣传的监听器列表。这个列表可能包括所有监听器，
   * 但也可能根据配置和需求进行筛选，例如，不包括用于控制器通信的监听器。
   *
   * @return 序列化的端点列表，表示有效的广告监听器。
   */
  def effectiveAdvertisedListeners: Seq[EndPoint] = {
    // 获取配置中指定的广告监听器属性
    val advertisedListenersProp = getString(KafkaConfig.AdvertisedListenersProp)
    // 如果广告监听器属性不为空
    if (advertisedListenersProp != null) {
      // 将监听器列表转换为端点列表
      CoreUtils.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, requireDistinctPorts=false)
    } else {
      // 如果广告监听器属性为空，筛选出不包括控制器监听器名的监听器
      listeners.filterNot(l => controllerListenerNames.contains(l.listenerName.value()))
    }
  }


  /**
   * 获取inter-broker监听器名称和安全协议。
   *
   * 此方法用于确定用于inter-broker通信的监听器名称和安全协议。它根据配置的不同情况，
   * 从以下两种方式中选择一种来决定监听器名称和安全协议：
   * 1. 如果设置了`inter.broker.listener.name`，则使用该名称并从`listener.security.protocol.map`中查找对应的安全协议。
   * 2. 如果没有设置`inter.broker.listener.name`，则直接使用`inter.broker.security.protocol`配置的安全协议。
   *
   * @return 一个包含监听器名称和安全协议的元组。
   * @throws ConfigException 如果同时设置了`inter.broker.listener.name`和`inter.broker.security.protocol`，或者
   *                         指定的监听器名称在`listener.security.protocol.map`中找不到对应的配置，则抛出此异常。
   */
  private def getInterBrokerListenerNameAndSecurityProtocol: (ListenerName, SecurityProtocol) = {
    // 检查是否同时设置了inter.broker.listener.name和inter.broker.security.protocol
    Option(getString(KafkaConfig.InterBrokerListenerNameProp)) match {
      case Some(_) if originals.containsKey(KafkaConfig.InterBrokerSecurityProtocolProp) =>
        // mark 如果同时设置了两者，则抛出ConfigException异常
        throw new ConfigException(s"Only one of ${KafkaConfig.InterBrokerListenerNameProp} and " +
          s"${KafkaConfig.InterBrokerSecurityProtocolProp} should be set.")
      case Some(name) =>
        // 如果设置了inter.broker.listener.name，获取其规范化名称，并从listener.security.protocol.map中查找对应的安全协议
        val listenerName = ListenerName.normalised(name)
        // mark 根据监听器名称获取对应的协议
        val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with name ${listenerName.value} defined in " +
            s"${KafkaConfig.InterBrokerListenerNameProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}."))
        (listenerName, securityProtocol)
      case None =>
        // mark 如果没有设置inter.broker.listener.name，直接使用inter.broker.security.protocol配置的安全协议(去默认值)
        // mark 并根据安全协议确定默认的监听器名称
        val securityProtocol = getSecurityProtocol(getString(KafkaConfig.InterBrokerSecurityProtocolProp),
          KafkaConfig.InterBrokerSecurityProtocolProp)
        (ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    }
  }


  /**
   * 获取控制面监听器名称及其安全协议。
   *
   * 此方法尝试从配置中获取控制面监听器的名称和安全协议。它首先检查是否存在
   * 控制面监听器名称的配置，如果存在，它将验证该监听器名称是否在配置的安全协议映射中定义。
   * 如果监听器名称不存在于映射中，将抛出配置异常。如果不存在控制面监听器名称的配置，
   * 则方法返回None。
   *
   * @return 可选的监听器名称和安全协议对。如果配置中没有定义控制面监听器名称，则为None。
   */
  private def getControlPlaneListenerNameAndSecurityProtocol: Option[(ListenerName, SecurityProtocol)] = {
    // mark 尝试从配置中获取控制面监听器名称
    Option(getString(KafkaConfig.ControlPlaneListenerNameProp)) match {
      case Some(name) =>
        // 标准化监听器名称
        val listenerName = ListenerName.normalised(name)
        // 从安全协议映射中获取监听器的安全协议，如果不存在则抛出异常
        val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with ${listenerName.value} defined in " +
            s"${KafkaConfig.ControlPlaneListenerNameProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}."))
        // 返回监听器名称和安全协议的组合
        Some(listenerName, securityProtocol)

      case None => None
    }
  }

  /**
   * 根据协议名称从默认协议映射获取协议名称。
   *
   * @param protocolName 协议名称，用于查找相应的安全协议。
   * @param configName   配置名称，用于在抛出异常时提供上下文信息。
   * @return 成功时返回对应的SecurityProtocol实例。
   * @throws ConfigException 如果协议名称无效，则抛出此异常。
   */
  private def getSecurityProtocol(protocolName: String, configName: String): SecurityProtocol = {
    try SecurityProtocol.forName(protocolName)
    catch {
      case _: IllegalArgumentException =>
        throw new ConfigException(s"Invalid security protocol `$protocolName` defined in $configName")
    }
  }
  /**
   * 获取有效监听器安全协议映射。
   *
   * 此方法根据配置确定每个监听器的安全协议。如果使用自管理集群（KRaft）模式，
   * 且未显式配置监听器安全协议映射，则会为控制器监听器添加默认的PLAINTEXT协议，
   * 除非已经配置了SSL或SASL协议。
   *
   * @return 监听器名称与其安全协议的映射。
   */
  def effectiveListenerSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = {
    // mark 从配置中获取监听器安全协议映射，并规范化监听器名称
    // mark getMap 方法会将 A:B:C,A1:B2:C1方法先按照逗号分隔然后再按照最后一个冒号分割生成一个map
    // mark 是用来处理 listener.security.protocol.map 比如 PLAINTEXT:PLAINTEXT,SSL:SSL 生成监听器->协议的映射
    // mark ListenerName 是对监听器名称包装, SecurityProtocol是安全协议的枚举对象
    val mapValue = getMap(KafkaConfig.ListenerSecurityProtocolMapProp, getString(KafkaConfig.ListenerSecurityProtocolMapProp))
      .map { case (listenerName, protocolName) =>
        ListenerName.normalised(listenerName) -> getSecurityProtocol(protocolName, KafkaConfig.ListenerSecurityProtocolMapProp)
      }

    // mark (kRrft模式)如果使用自管理集群模式，并且没有显式配置监听器安全协议映射
    if (usesSelfManagedQuorum && !originals.containsKey(ListenerSecurityProtocolMapProp)) {
      // 定义一个方法来检查是否使用了SSL或SASL协议
      def isSslOrSasl(name: String): Boolean = name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name)

      // 如果控制器监听器或监听器列表中使用了SSL或SASL协议，则不添加默认映射
      // 否则，为所有不是PLAINTEXT的控制器监听器添加PLAINTEXT协议映射
      if (controllerListenerNames.exists(isSslOrSasl) ||
        parseCsvList(getString(KafkaConfig.ListenersProp)).exists(listenerValue => isSslOrSasl(EndPoint.parseListenerName(listenerValue)))) {
        mapValue // 不添加默认映射
      } else {
        // 添加默认的PLAINTEXT映射
        mapValue ++ controllerListenerNames.filterNot(SecurityProtocol.PLAINTEXT.name.equals(_)).map(
          new ListenerName(_) -> SecurityProtocol.PLAINTEXT)
      }
    } else {
      // 返回原始映射
      mapValue
    }
  }


  // Topic IDs are used with all self-managed quorum clusters and ZK cluster with IBP greater than or equal to 2.8
  def usesTopicId: Boolean =
    usesSelfManagedQuorum || interBrokerProtocolVersion.isTopicIdsSupported()

  validateValues()

  /**
   * 验证配置值的有效性。
   * 此方法检查配置是否符合运行要求，包括但不限于：
   * - nodeId 和 brokerId 的一致性
   * - ZooKeeper 配置的完整性
   * - KRaft 模式下的配置要求
   * - 日志滚动和保留时间的合理性
   * - 日志目录和清洁器缓冲区的配置
   * - 复制和偏移提交的配置
   * - 压缩类型的有效性
   * - 听众名称和协议的匹配性
   *
   * @throws ConfigException 如果配置不满足要求
   */
  @nowarn("cat=deprecation")
  private def validateValues(): Unit = {
    if (nodeId != brokerId) {
      throw new ConfigException(s"You must set `${KafkaConfig.NodeIdProp}` to the same value as `${KafkaConfig.BrokerIdProp}`.")
    }
    if (requiresZookeeper) {
      if (zkConnect == null) {
        throw new ConfigException(s"Missing required configuration `${KafkaConfig.ZkConnectProp}` which has no default value.")
      }
      if (brokerIdGenerationEnable) {
        require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id")
      } else {
        require(brokerId >= 0, "broker.id must be greater than or equal to 0")
      }
    }
    else {
      // KRaft-based metadata quorum
      if (nodeId < 0) {
        throw new ConfigException(s"Missing configuration `${KafkaConfig.NodeIdProp}` which is required " +
          s"when `process.roles` is defined (i.e. when running in KRaft mode).")
      }
      if (migrationEnabled) {
        if (zkConnect == null) {
          throw new ConfigException(s"If using `${KafkaConfig.MigrationEnabledProp}` in KRaft mode, `${KafkaConfig.ZkConnectProp}` must also be set.")
        }
      }
    }
    require(logRollTimeMillis >= 1, "log.roll.ms must be greater than or equal to 1")
    require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be greater than or equal to 0")
    require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1")
    require(logDirs.nonEmpty, "At least one log directory must be defined via log.dirs or log.dir.")
    require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
    require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
      " to prevent unnecessary socket timeouts")
    require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
      " to prevent frequent changes in ISR")
    require(offsetCommitRequiredAcks >= -1 && offsetCommitRequiredAcks <= offsetsTopicReplicationFactor,
      "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor")
    require(BrokerCompressionCodec.isValid(compressionType), "compression.type : " + compressionType + " is not valid." +
      " Valid options are " + BrokerCompressionCodec.brokerCompressionOptions.mkString(","))
    val advertisedListenerNames = effectiveAdvertisedListeners.map(_.listenerName).toSet

    // validate KRaft-related configs
    val voterAddressSpecsByNodeId = RaftConfig.parseVoterConnections(quorumVoters)
    def validateNonEmptyQuorumVotersForKRaft(): Unit = {
      if (voterAddressSpecsByNodeId.isEmpty) {
        throw new ConfigException(s"If using ${KafkaConfig.ProcessRolesProp}, ${KafkaConfig.QuorumVotersProp} must contain a parseable set of voters.")
      }
    }
    def validateNonEmptyQuorumVotersForMigration(): Unit = {
      if (voterAddressSpecsByNodeId.isEmpty) {
        throw new ConfigException(s"If using ${KafkaConfig.MigrationEnabledProp}, ${KafkaConfig.QuorumVotersProp} must contain a parseable set of voters.")
      }
    }
    def validateControlPlaneListenerEmptyForKRaft(): Unit = {
      require(controlPlaneListenerName.isEmpty,
        s"${KafkaConfig.ControlPlaneListenerNameProp} is not supported in KRaft mode.")
    }
    def validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(): Unit = {
      require(!advertisedListenerNames.exists(aln => controllerListenerNames.contains(aln.value())),
        s"The advertised.listeners config must not contain KRaft controller listeners from ${KafkaConfig.ControllerListenerNamesProp} when ${KafkaConfig.ProcessRolesProp} contains the broker role because Kafka clients that send requests via advertised listeners do not send requests to KRaft controllers -- they only send requests to KRaft brokers.")
    }
    def validateControllerQuorumVotersMustContainNodeIdForKRaftController(): Unit = {
      require(voterAddressSpecsByNodeId.containsKey(nodeId),
        s"If ${KafkaConfig.ProcessRolesProp} contains the 'controller' role, the node id $nodeId must be included in the set of voters ${KafkaConfig.QuorumVotersProp}=${voterAddressSpecsByNodeId.asScala.keySet.toSet}")
    }
    def validateControllerListenerExistsForKRaftController(): Unit = {
      require(controllerListeners.nonEmpty,
        s"${KafkaConfig.ControllerListenerNamesProp} must contain at least one value appearing in the '${KafkaConfig.ListenersProp}' configuration when running the KRaft controller role")
    }
    def validateControllerListenerNamesMustAppearInListenersForKRaftController(): Unit = {
      val listenerNameValues = listeners.map(_.listenerName.value).toSet
      require(controllerListenerNames.forall(cln => listenerNameValues.contains(cln)),
        s"${KafkaConfig.ControllerListenerNamesProp} must only contain values appearing in the '${KafkaConfig.ListenersProp}' configuration when running the KRaft controller role")
    }
    def validateAdvertisedListenersNonEmptyForBroker(): Unit = {
      require(advertisedListenerNames.nonEmpty,
        "There must be at least one advertised listener." + (
          if (processRoles.contains(BrokerRole)) s" Perhaps all listeners appear in ${ControllerListenerNamesProp}?" else ""))
    }
    if (processRoles == Set(BrokerRole)) {
      // KRaft broker-only
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
      // nodeId must not appear in controller.quorum.voters
      require(!voterAddressSpecsByNodeId.containsKey(nodeId),
        s"If ${KafkaConfig.ProcessRolesProp} contains just the 'broker' role, the node id $nodeId must not be included in the set of voters ${KafkaConfig.QuorumVotersProp}=${voterAddressSpecsByNodeId.asScala.keySet.toSet}")
      // controller.listener.names must be non-empty...
      require(controllerListenerNames.nonEmpty,
        s"${KafkaConfig.ControllerListenerNamesProp} must contain at least one value when running KRaft with just the broker role")
      // controller.listener.names are forbidden in listeners...
      require(controllerListeners.isEmpty,
        s"${KafkaConfig.ControllerListenerNamesProp} must not contain a value appearing in the '${KafkaConfig.ListenersProp}' configuration when running KRaft with just the broker role")
      // controller.listener.names must all appear in listener.security.protocol.map
      controllerListenerNames.foreach { name =>
        val listenerName = ListenerName.normalised(name)
        if (!effectiveListenerSecurityProtocolMap.contains(listenerName)) {
          throw new ConfigException(s"Controller listener with name ${listenerName.value} defined in " +
            s"${KafkaConfig.ControllerListenerNamesProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}  (an explicit security mapping for each controller listener is required if ${KafkaConfig.ListenerSecurityProtocolMapProp} is non-empty, or if there are security protocols other than PLAINTEXT in use)")
        }
      }
      // warn that only the first controller listener is used if there is more than one
      if (controllerListenerNames.size > 1) {
        warn(s"${KafkaConfig.ControllerListenerNamesProp} has multiple entries; only the first will be used since ${KafkaConfig.ProcessRolesProp}=broker: ${controllerListenerNames.asJava}")
      }
      validateAdvertisedListenersNonEmptyForBroker()
    }
    else if (processRoles == Set(ControllerRole)) {
      // KRaft controller-only
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      // advertised listeners must be empty when only the controller is configured
      require(
        getString(KafkaConfig.AdvertisedListenersProp) == null,
        s"The ${KafkaConfig.AdvertisedListenersProp} config must be empty when ${KafkaConfig.ProcessRolesProp}=controller"
      )
      // listeners should only contain listeners also enumerated in the controller listener
      require(
        effectiveAdvertisedListeners.isEmpty,
        s"The ${KafkaConfig.ListenersProp} config must only contain KRaft controller listeners from ${KafkaConfig.ControllerListenerNamesProp} when ${KafkaConfig.ProcessRolesProp}=controller"
      )
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateControllerListenerExistsForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
    }
    else if (isKRaftCoResidentMode) {
      // KRaft colocated broker and controller
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateControllerListenerExistsForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
      validateAdvertisedListenersNonEmptyForBroker()
    }
    else {
      // ZK-based
      if (migrationEnabled) {
        require(!originals.containsKey(KafkaConfig.AuthorizerClassNameProp),
          s"ZooKeeper migration does not yet support authorizers. Remove ${KafkaConfig.AuthorizerClassNameProp} before performing a migration.")
        validateNonEmptyQuorumVotersForMigration()
        require(controllerListenerNames.nonEmpty,
          s"${KafkaConfig.ControllerListenerNamesProp} must not be empty when running in ZooKeeper migration mode: ${controllerListenerNames.asJava}")
        require(interBrokerProtocolVersion.isMigrationSupported, s"Cannot enable ZooKeeper migration without setting " +
          s"'${KafkaConfig.InterBrokerProtocolVersionProp}' to 3.4 or higher")
        require(logDirs.size == 1, "Cannot enable ZooKeeper migration when multiple log directories (aka JBOD) are in use.")
      } else {
        // controller listener names must be empty when not in KRaft mode
        require(controllerListenerNames.isEmpty,
          s"${KafkaConfig.ControllerListenerNamesProp} must be empty when not running in KRaft mode: ${controllerListenerNames.asJava}")
      }
      validateAdvertisedListenersNonEmptyForBroker()
    }

    val listenerNames = listeners.map(_.listenerName).toSet
    if (processRoles.isEmpty || processRoles.contains(BrokerRole)) {
      // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
      validateAdvertisedListenersNonEmptyForBroker()
      require(advertisedListenerNames.contains(interBrokerListenerName),
        s"${KafkaConfig.InterBrokerListenerNameProp} must be a listener name defined in ${KafkaConfig.AdvertisedListenersProp}. " +
          s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
      require(advertisedListenerNames.subsetOf(listenerNames),
        s"${KafkaConfig.AdvertisedListenersProp} listener names must be equal to or a subset of the ones defined in ${KafkaConfig.ListenersProp}. " +
          s"Found ${advertisedListenerNames.map(_.value).mkString(",")}. The valid options based on the current configuration " +
          s"are ${listenerNames.map(_.value).mkString(",")}"
      )
    }

    require(!effectiveAdvertisedListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
      s"${KafkaConfig.AdvertisedListenersProp} cannot use the nonroutable meta-address 0.0.0.0. "+
      s"Use a routable IP address.")

    // validate control.plane.listener.name config
    if (controlPlaneListenerName.isDefined) {
      require(advertisedListenerNames.contains(controlPlaneListenerName.get),
        s"${KafkaConfig.ControlPlaneListenerNameProp} must be a listener name defined in ${KafkaConfig.AdvertisedListenersProp}. " +
        s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
      // controlPlaneListenerName should be different from interBrokerListenerName
      require(!controlPlaneListenerName.get.value().equals(interBrokerListenerName.value()),
        s"${KafkaConfig.ControlPlaneListenerNameProp}, when defined, should have a different value from the inter broker listener name. " +
        s"Currently they both have the value ${controlPlaneListenerName.get}")
    }

    val messageFormatVersion = new MessageFormatVersion(logMessageFormatVersionString, interBrokerProtocolVersionString)
    if (messageFormatVersion.shouldWarn)
      warn(messageFormatVersion.brokerWarningMessage)

    val recordVersion = logMessageFormatVersion.highestSupportedRecordVersion
    require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= recordVersion.value,
      s"log.message.format.version $logMessageFormatVersionString can only be used when inter.broker.protocol.version " +
      s"is set to version ${MetadataVersion.minSupportedFor(recordVersion).shortVersion} or higher")

    if (offsetsTopicCompressionCodec == ZStdCompressionCodec)
      require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= IBP_2_1_IV0.highestSupportedRecordVersion().value,
        "offsets.topic.compression.codec zstd can only be used when inter.broker.protocol.version " +
        s"is set to version ${IBP_2_1_IV0.shortVersion} or higher")

    val interBrokerUsesSasl = interBrokerSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol == SecurityProtocol.SASL_SSL
    require(!interBrokerUsesSasl || saslInterBrokerHandshakeRequestEnable || saslMechanismInterBrokerProtocol == SaslConfigs.GSSAPI_MECHANISM,
      s"Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to $interBrokerProtocolVersionString")
    require(!interBrokerUsesSasl || saslEnabledMechanisms(interBrokerListenerName).contains(saslMechanismInterBrokerProtocol),
      s"${KafkaConfig.SaslMechanismInterBrokerProtocolProp} must be included in ${KafkaConfig.SaslEnabledMechanismsProp} when SASL is used for inter-broker communication")
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${KafkaConfig.QueuedMaxBytesProp} must be larger or equal to ${KafkaConfig.SocketRequestMaxBytesProp}")

    if (maxConnectionsPerIp == 0)
      require(!maxConnectionsPerIpOverrides.isEmpty, s"${KafkaConfig.MaxConnectionsPerIpProp} can be set to zero only if" +
        s" ${KafkaConfig.MaxConnectionsPerIpOverridesProp} property is set.")

    val invalidAddresses = maxConnectionsPerIpOverrides.keys.filterNot(address => Utils.validHostPattern(address))
    if (!invalidAddresses.isEmpty)
      throw new IllegalArgumentException(s"${KafkaConfig.MaxConnectionsPerIpOverridesProp} contains invalid addresses : ${invalidAddresses.mkString(",")}")

    if (connectionsMaxIdleMs >= 0)
      require(failedAuthenticationDelayMs < connectionsMaxIdleMs,
        s"${KafkaConfig.FailedAuthenticationDelayMsProp}=$failedAuthenticationDelayMs should always be less than" +
        s" ${KafkaConfig.ConnectionsMaxIdleMsProp}=$connectionsMaxIdleMs to prevent failed" +
        s" authentication responses from timing out")

    val principalBuilderClass = getClass(KafkaConfig.PrincipalBuilderClassProp)
    require(principalBuilderClass != null, s"${KafkaConfig.PrincipalBuilderClassProp} must be non-null")
    require(classOf[KafkaPrincipalSerde].isAssignableFrom(principalBuilderClass),
      s"${KafkaConfig.PrincipalBuilderClassProp} must implement KafkaPrincipalSerde")
  }
}
