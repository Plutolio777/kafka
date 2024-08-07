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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

public final class DefaultSslEngineFactory implements SslEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultSslEngineFactory.class);
    public static final String PEM_TYPE = "PEM";

    private Map<String, ?> configs;
    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore;
    private SecurityStore truststore;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private SecureRandom secureRandomImplementation;
    private SSLContext sslContext;
    private SslClientAuth sslClientAuth;


    /**
     * 创建用于客户端的SSLEngine。
     * 此方法旨在根据提供的参数配置并创建一个SSLEngine，该SSLEngine将用于客户端模式下的SSL/TLS通信。
     * 它通过调用另一个方法 {@code createSslEngine} 来实现具体的创建逻辑，传递客户端模式及相关参数以配置SSLEngine。
     *
     * @param peerHost               目标主机的名称或IP地址，客户端将与此主机建立SSL/TLS连接。
     * @param peerPort               目标主机的端口号，客户端将与此端口建立SSL/TLS连接。
     * @param endpointIdentification 用于验证对端主机标识的算法，可以为空。如果非空，将根据指定的算法验证对端主机的标识。
     * @return 返回配置好的SSLEngine，该引擎已准备好用于客户端模式下的SSL/TLS通信。
     */
    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        return createSslEngine(Mode.CLIENT, peerHost, peerPort, endpointIdentification);
    }

    /**
     * 创建用于服务器端的SSLEngine。
     * 此方法旨在生成一个配置为服务器模式的SSLEngine，用于在安全的套接字上进行通信。
     * 它通过调用 {@code createSslEngine} 方法来实现，传入特定的模式、对等主机信息和对等端口。
     *
     * @param peerHost 对等主机的名称或IP地址。服务器将验证连接请求的客户端所声称的主机名是否与这个参数匹配。
     * @param peerPort 对等端口号，指定客户端试图连接的服务器端口。
     * @return 返回一个配置为服务器模式的SSLEngine。
     */
    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        // 通过调用createSslEngine方法，传入服务器模式以及其他必要参数来创建SSLEngine。
        return createSslEngine(Mode.SERVER, peerHost, peerPort, null);
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        if (!nextConfigs.equals(configs)) {
            return true;
        }
        if (truststore != null && truststore.modified()) {
            return true;
        }
        if (keystore != null && keystore.modified()) {
            return true;
        }
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public KeyStore keystore() {
        return this.keystore != null ? this.keystore.get() : null;
    }

    @Override
    public KeyStore truststore() {
        return this.truststore != null ? this.truststore.get() : null;
    }

    /**
     * 配置SSL上下文和相关参数。
     * 从提供的配置映射中提取SSL配置，并据此配置SSL上下文。
     * 此方法还负责创建和配置密钥和信任存储。
     *
     * @param configs 配置映射，包含SSL配置的关键-值对。
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
        // mark 获取ssl.protocol配置
        // mark 改配置用于生成SSLContext java11以上采用TLSv1.3 否则采用TLSv1.2
        // mark 最新的JVM支持 “TLSv1.2”和“TLSv1.3”。 “TLS”、“TLSv1.1”、“SSL”、“SSLv2”和“SSLv3”
        // mark 由于一直的安全漏洞kafka建议使用TLS 如果服务不支持TLS 1.3会自动降级称TLS1.2
        this.protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        // mark 获取ssl.provider
        // mark 用于指定 用于 SSL 连接的安全提供程序的名称。默认值是 JVM 的默认安全提供程序。
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);

        // mark 配置安全提供者
        SecurityUtils.addConfiguredSecurityProviders(this.configs);

        // mark 获取 ssl.cipher.suites
        // mark 这个配置定义了用于加密通信的算法组合，包括密钥交换算法、数据加密算法、消息认证算法等。配置这些套件可以帮助确保数据传输的安全性。
        // mark 常见的加密算法套件
        // mark 1.TLS_AES_128_GCM_SHA256：使用 AES-GCM 算法进行数据加密，SHA-256 进行消息认证。
        // mark 2.TLS_AES_256_GCM_SHA384：使用 AES-GCM 算法进行数据加密，SHA-384 进行消息认证。
        // mark 3.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256：使用 ECDHE 密钥交换，RSA 签名，AES-GCM 数据加密，SHA-256 消息认证。
        // mark 4.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA：使用 ECDHE 密钥交换，RSA 签名，AES-CBC 数据加密，SHA-1 消息认证。
        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty()) {
            this.cipherSuites = cipherSuitesList.toArray(new String[0]);
        } else {
            this.cipherSuites = null;
        }

        // mark ssl.enabled.protocols启动的协议列表 默认为
        // mark 如果使用 Java 11 或更新版本，默认值为 'TLSv1.2,TLSv1.3'，否则默认为 'TLSv1.2'。
        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty()) {
            this.enabledProtocols = enabledProtocolsList.toArray(new String[0]);
        } else {
            this.enabledProtocols = null;
        }

        // mark 提取ssl.secure.random.implementation配置
        // mark 指定用于 SSL 加密操作的 SecureRandom 随机数生成器实现类
        this.secureRandomImplementation = createSecureRandom((String)
                configs.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG));

        // mark 提取ssl.client.auth配置
        // mark 通常用于指定是否需要客户端的 SSL/TLS 证书进行身份验证。
        this.sslClientAuth = createSslClientAuth((String) configs.get(
                BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));

        // mark 获取ssl.keymanager.algorithm配置
        // mark 定制密钥管理器工厂实现类
        this.kmfAlgorithm = (String) configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        // mark  获取ssl.trust manager.algorithm配置
        // mark 定制trust管理工厂实现类
        this.tmfAlgorithm = (String) configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        // mark 提取keystore相关配置
        // mark 1.ssl.keystore.type 指定keystore文件格式 支持的值包括 [JKS, PKCS12, PEM]
        // mark 2.ssl.keystore.location 指定keystore文件地址
        // mark 3.ssl.keystore.password 指定keystore的密码 PEM格式不需要密码
        // mark 4.ssl.keystore.key 按照 'ssl.keystore.type' 指定的格式提供的私钥。
        // mark 5.ssl.keystore.certificate.chain 按照 ssl.keystore.type' 指定的格式提供的证书链。
        //  默认的 SSL 引擎工厂仅支持 PEM 格式的 X.509 证书链列表。
        this.keystore = createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));

        // mark 提取truststore相关配置
        // mark 1.ssl.truststore.type 指定keystore文件格式 支持的值包括 [JKS, PKCS12, PEM]
        // mark 2.ssl.truststore.location 指定truststore文件地址
        // mark 3.ssl.truststore.password 指定keystore的密码 PEM格式不需要密码
        // mark 4.ssl.truststore.certificates 按照 ssl.keystore.type' 指定的格式提供的证书链。
        //  默认的 SSL 引擎工厂仅支持 PEM 格式的 X.509 证书链列表。
        this.truststore = createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));

        // mark 生成 SSL Context
        this.sslContext = createSSLContext(keystore, truststore);
    }

    @Override
    public void close() {
        this.sslContext = null;
    }

    //For Test only
    public SSLContext sslContext() {
        return this.sslContext;
    }

    /**
     * 根据指定的模式和参数创建并配置SSLEngine。
     * SSLEngine用于安全套接字通信，其配置由模式（客户端或服务器）、
     * 以及诸如对等方的主机名、端口号和所需客户端身份验证级别等其他参数决定。
     *
     * @param mode SSLEngine的操作模式，可以是SERVER或CLIENT。
     * @param peerHost 对等方的主机名，用于SSL握手。
     * @param peerPort 对等方的端口号，用于SSL握手。
     * @param endpointIdentification 端点识别算法，用于验证远程端点的身份。
     *                              当模式设置为CLIENT时，通常会使用此参数来确保连接到预期的服务器。
     * @return 根据指定参数初始化的SSLEngine。
     */
    private SSLEngine createSslEngine(Mode mode, String peerHost, int peerPort, String endpointIdentification) {
        // mark 创建SSLEngine并初始化其对等信息。
        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);

        // mark 设置可用的密码套件和协议。
        if (cipherSuites != null) sslEngine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);


        if (mode == Mode.SERVER) {
            // mark 配置SSLEngine为服务器模式。
            sslEngine.setUseClientMode(false);
            // mark 根据配置设置客户端认证需求。
            switch (sslClientAuth) {
                case REQUIRED:
                    sslEngine.setNeedClientAuth(true);
                    break;
                case REQUESTED:
                    sslEngine.setWantClientAuth(true);
                    break;
                case NONE:
                    break;
            }
            sslEngine.setUseClientMode(false);
        } else {
            // mark 配置SSLEngine为客户端模式。
            sslEngine.setUseClientMode(true);
            SSLParameters sslParams = sslEngine.getSSLParameters();
            // mark 设置端点识别算法，仅在客户端模式下启用端点验证。
            sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
            sslEngine.setSSLParameters(sslParams);
        }

        // mark 返回配置好的SSLEngine。
        return sslEngine;
    }


    /**
     * 根据提供的配置键创建SslClientAuth实例。 SslClientAuth是个枚举值表示服务端是否需要验证客户端上身份
     *
     * @param key 用于确定SSL客户端身份验证策略的配置键。
     * @return 代表相应身份验证策略的SslClientAuth实例。
     */
    private static SslClientAuth createSslClientAuth(String key) {
        // mark 根据字符串获取枚举值
        SslClientAuth auth = SslClientAuth.forConfig(key);
        if (auth != null) {
            // 如果获得的实例不为空，则直接返回对应的身份验证策略
            return auth;
        }
        log.warn("Unrecognized client authentication configuration {}.  Falling " +
                "back to NONE.  Recognized client authentication configurations are {}.",
                key, String.join(", ", SslClientAuth.VALUES.stream().
                        map(Enum::name).collect(Collectors.toList())));
        return SslClientAuth.NONE;
    }


    /**
     * 根据提供的算法名称创建一个 SecureRandom 实例。
     * SecureRandom 是用于生成安全随机数的类。它被用于需要强随机性的场景，比如密码学应用。
     *
     * @param key 算法名称，用于指定要创建的 SecureRandom 实例的算法。
     *            如果 key 为 null，则返回 null。
     * @return 返回一个使用指定算法的 SecureRandom 实例，如果 key 为 null，则返回 null。
     * @throws KafkaException 如果无法实例化 SecureRandom，将抛出 KafkaException。
     *                         这种情况通常发生在指定的算法不支持或者由于其他安全原因无法初始化时。
     */
    private static SecureRandom createSecureRandom(String key) {
        // mark 检查 key 是否为 null
        if (key == null) {
            return null;
        }
        try {
            // mark 尝试根据提供的算法名称实例化 SecureRandom
            return SecureRandom.getInstance(key);
        } catch (GeneralSecurityException e) {
            // 如果实例化失败，抛出 KafkaException，并将原始异常作为原因
            throw new KafkaException(e);
        }
    }

    /**
     * 创建一个SSLContext实例，该实例用于配置SSL/TLS的安全参数。
     *
     * @param keystore 安全存储，包含服务器的密钥和证书。
     * @param truststore 信任存储，包含客户端信任的证书。
     * @return 初始化后的SSLContext对象。
     * @throws KafkaException 如果初始化SSLContext过程中发生异常。
     */
    private SSLContext createSSLContext(SecurityStore keystore, SecurityStore truststore) {
        try {
            // mark 生成SSLContext实例
            SSLContext sslContext;
            if (provider != null)
                sslContext = SSLContext.getInstance(protocol, provider);
            else
                sslContext = SSLContext.getInstance(protocol);

            KeyManager[] keyManagers = null;
            if (keystore != null || kmfAlgorithm != null) {
                // mark 生成秘钥管理工厂
                String kmfAlgorithm = this.kmfAlgorithm != null ?
                        this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
                if (keystore != null) {
                    kmf.init(keystore.get(), keystore.keyPassword());
                } else {
                    kmf.init(null, null);
                }
                keyManagers = kmf.getKeyManagers();
            }

            // mark 生成trust管理工厂
            String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            KeyStore ts = truststore == null ? null : truststore.get();
            tmf.init(ts);

            // mark 初始化SSLContext
            sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
            log.debug("Created SSL context with keystore {}, truststore {}, provider {}.",
                    keystore, truststore, sslContext.getProvider().getName());
            return sslContext;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    // Visibility to override for testing
    protected SecurityStore createKeystore(String type, String path, Password password, Password keyPassword, Password privateKey, Password certificateChain) {
        if (privateKey != null) {
            if (!PEM_TYPE.equals(type))
                throw new InvalidConfigurationException("SSL private key can be specified only for PEM, but key store type is " + type + ".");
            else if (certificateChain == null)
                throw new InvalidConfigurationException("SSL private key is specified, but certificate chain is not specified.");
            else if (path != null)
                throw new InvalidConfigurationException("Both SSL key store location and separate private key are specified.");
            else if (password != null)
                throw new InvalidConfigurationException("SSL key store password cannot be specified with PEM format, only key password may be specified.");
            else
                return new PemStore(certificateChain, privateKey, keyPassword);
        } else if (certificateChain != null) {
            throw new InvalidConfigurationException("SSL certificate chain is specified, but private key is not specified");
        } else if (PEM_TYPE.equals(type) && path != null) {
            if (password != null)
                throw new InvalidConfigurationException("SSL key store password cannot be specified with PEM format, only key password may be specified");
            else
                return new FileBasedPemStore(path, keyPassword, true);
        } else if (path == null && password != null) {
            throw new InvalidConfigurationException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new InvalidConfigurationException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            return new FileBasedStore(type, path, password, keyPassword, true);
        } else
            return null; // path == null, clients may use this path with brokers that don't require client auth
    }

    private static SecurityStore createTruststore(String type, String path, Password password, Password trustStoreCerts) {
        if (trustStoreCerts != null) {
            if (!PEM_TYPE.equals(type))
                throw new InvalidConfigurationException("SSL trust store certs can be specified only for PEM, but trust store type is " + type + ".");
            else if (path != null)
                throw new InvalidConfigurationException("Both SSL trust store location and separate trust certificates are specified.");
            else if (password != null)
                throw new InvalidConfigurationException("SSL trust store password cannot be specified for PEM format.");
            else
                return new PemStore(trustStoreCerts);
        } else if (PEM_TYPE.equals(type) && path != null) {
            if (password != null)
                throw new InvalidConfigurationException("SSL trust store password cannot be specified for PEM format.");
            else
                return new FileBasedPemStore(path, null, false);
        } else if (path == null && password != null) {
            throw new InvalidConfigurationException("SSL trust store is not specified, but trust store password is specified.");
        } else if (path != null) {
            return new FileBasedStore(type, path, password, null, false);
        } else
            return null;
    }

    interface SecurityStore {
        KeyStore get();
        char[] keyPassword();
        boolean modified();
    }

    // package access for testing
    static class FileBasedStore implements SecurityStore {
        private final String type;
        protected final String path;
        private final Password password;
        protected final Password keyPassword;
        private final Long fileLastModifiedMs;
        private final KeyStore keyStore;

        FileBasedStore(String type, String path, Password password, Password keyPassword, boolean isKeyStore) {
            Objects.requireNonNull(type, "type must not be null");
            this.type = type;
            this.path = path;
            this.password = password;
            this.keyPassword = keyPassword;
            fileLastModifiedMs = lastModifiedMs(path);
            this.keyStore = load(isKeyStore);
        }

        @Override
        public KeyStore get() {
            return keyStore;
        }

        @Override
        public char[] keyPassword() {
            Password passwd = keyPassword != null ? keyPassword : password;
            return passwd == null ? null : passwd.value().toCharArray();
        }

        /**
         * Loads this keystore
         * @return the keystore
         * @throws KafkaException if the file could not be read or if the keystore could not be loaded
         *   using the specified configs (e.g. if the password or keystore type is invalid)
         */
        protected KeyStore load(boolean isKeyStore) {
            try (InputStream in = Files.newInputStream(Paths.get(path))) {
                KeyStore ks = KeyStore.getInstance(type);
                // If a password is not set access to the truststore is still available, but integrity checking is disabled.
                char[] passwordChars = password != null ? password.value().toCharArray() : null;
                ks.load(in, passwordChars);
                return ks;
            } catch (GeneralSecurityException | IOException e) {
                throw new KafkaException("Failed to load SSL keystore " + path + " of type " + type, e);
            }
        }

        private Long lastModifiedMs(String path) {
            try {
                return Files.getLastModifiedTime(Paths.get(path)).toMillis();
            } catch (IOException e) {
                log.error("Modification time of key store could not be obtained: " + path, e);
                return null;
            }
        }

        public boolean modified() {
            Long modifiedMs = lastModifiedMs(path);
            return modifiedMs != null && !Objects.equals(modifiedMs, this.fileLastModifiedMs);
        }

        @Override
        public String toString() {
            return "SecurityStore(" +
                    "path=" + path +
                    ", modificationTime=" + (fileLastModifiedMs == null ? null : new Date(fileLastModifiedMs)) + ")";
        }
    }

    static class FileBasedPemStore extends FileBasedStore {
        FileBasedPemStore(String path, Password keyPassword, boolean isKeyStore) {
            super(PEM_TYPE, path, null, keyPassword, isKeyStore);
        }

        @Override
        protected KeyStore load(boolean isKeyStore) {
            try {
                Password storeContents = new Password(Utils.readFileAsString(path));
                PemStore pemStore = isKeyStore ? new PemStore(storeContents, storeContents, keyPassword) :
                    new PemStore(storeContents);
                return pemStore.keyStore;
            } catch (Exception e) {
                throw new InvalidConfigurationException("Failed to load PEM SSL keystore " + path, e);
            }
        }
    }

    static class PemStore implements SecurityStore {
        private static final PemParser CERTIFICATE_PARSER = new PemParser("CERTIFICATE");
        private static final PemParser PRIVATE_KEY_PARSER = new PemParser("PRIVATE KEY");
        private static final List<KeyFactory> KEY_FACTORIES = Arrays.asList(
                keyFactory("RSA"),
                keyFactory("DSA"),
                keyFactory("EC")
        );

        private final char[] keyPassword;
        private final KeyStore keyStore;

        PemStore(Password certificateChain, Password privateKey, Password keyPassword) {
            this.keyPassword = keyPassword == null ? null : keyPassword.value().toCharArray();
            keyStore = createKeyStoreFromPem(privateKey.value(), certificateChain.value(), this.keyPassword);
        }

        PemStore(Password trustStoreCerts) {
            this.keyPassword = null;
            keyStore = createTrustStoreFromPem(trustStoreCerts.value());
        }

        @Override
        public KeyStore get() {
            return keyStore;
        }

        @Override
        public char[] keyPassword() {
            return keyPassword;
        }

        @Override
        public boolean modified() {
            return false;
        }

        private KeyStore createKeyStoreFromPem(String privateKeyPem, String certChainPem, char[] keyPassword) {
            try {
                KeyStore ks = KeyStore.getInstance("PKCS12");
                ks.load(null, null);
                Key key = privateKey(privateKeyPem, keyPassword);
                Certificate[] certChain = certs(certChainPem);
                ks.setKeyEntry("kafka", key, keyPassword, certChain);
                return ks;
            } catch (Exception e) {
                throw new InvalidConfigurationException("Invalid PEM keystore configs", e);
            }
        }

        private KeyStore createTrustStoreFromPem(String trustedCertsPem) {
            try {
                KeyStore ts = KeyStore.getInstance("PKCS12");
                ts.load(null, null);
                Certificate[] certs = certs(trustedCertsPem);
                for (int i = 0; i < certs.length; i++) {
                    ts.setCertificateEntry("kafka" + i, certs[i]);
                }
                return ts;
            } catch (InvalidConfigurationException e) {
                throw e;
            } catch (Exception e) {
                throw new InvalidConfigurationException("Invalid PEM truststore configs", e);
            }
        }

        private Certificate[] certs(String pem) throws GeneralSecurityException {
            List<byte[]> certEntries = CERTIFICATE_PARSER.pemEntries(pem);
            if (certEntries.isEmpty())
                throw new InvalidConfigurationException("At least one certificate expected, but none found");

            Certificate[] certs = new Certificate[certEntries.size()];
            for (int i = 0; i < certs.length; i++) {
                certs[i] = CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(certEntries.get(i)));
            }
            return certs;
        }

        private PrivateKey privateKey(String pem, char[] keyPassword) throws Exception {
            List<byte[]> keyEntries = PRIVATE_KEY_PARSER.pemEntries(pem);
            if (keyEntries.isEmpty())
                throw new InvalidConfigurationException("Private key not provided");
            if (keyEntries.size() != 1)
                throw new InvalidConfigurationException("Expected one private key, but found " + keyEntries.size());

            byte[] keyBytes = keyEntries.get(0);
            PKCS8EncodedKeySpec keySpec;
            if (keyPassword == null) {
                keySpec = new PKCS8EncodedKeySpec(keyBytes);
            } else {
                EncryptedPrivateKeyInfo keyInfo = new EncryptedPrivateKeyInfo(keyBytes);
                String algorithm = keyInfo.getAlgName();
                SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(algorithm);
                SecretKey pbeKey = keyFactory.generateSecret(new PBEKeySpec(keyPassword));
                Cipher cipher = Cipher.getInstance(algorithm);
                cipher.init(Cipher.DECRYPT_MODE, pbeKey, keyInfo.getAlgParameters());
                keySpec = keyInfo.getKeySpec(cipher);
            }

            InvalidKeySpecException firstException = null;
            for (KeyFactory factory : KEY_FACTORIES) {
                try {
                    return factory.generatePrivate(keySpec);
                } catch (InvalidKeySpecException e) {
                    if (firstException == null)
                        firstException = e;
                }
            }
            throw new InvalidConfigurationException("Private key could not be loaded", firstException);
        }

        private static KeyFactory keyFactory(String algorithm) {
            try {
                return KeyFactory.getInstance(algorithm);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Could not create key factory for algorithm " + algorithm, e);
            }
        }
    }

    /**
     * Parser to process certificate/private key entries from PEM files
     * Examples:
     *   -----BEGIN CERTIFICATE-----
     *   Base64 cert
     *   -----END CERTIFICATE-----
     *
     *   -----BEGIN ENCRYPTED PRIVATE KEY-----
     *   Base64 private key
     *   -----END ENCRYPTED PRIVATE KEY-----
     *   Additional data may be included before headers, so we match all entries within the PEM.
     */
    static class PemParser {
        private final String name;
        private final Pattern pattern;

        PemParser(String name) {
            this.name = name;
            String beginOrEndFormat = "-+%s\\s*.*%s[^-]*-+\\s+";
            String nameIgnoreSpace = name.replace(" ", "\\s+");

            String encodingParams = "\\s*[^\\r\\n]*:[^\\r\\n]*[\\r\\n]+";
            String base64Pattern = "([a-zA-Z0-9/+=\\s]*)";
            String patternStr =  String.format(beginOrEndFormat, "BEGIN", nameIgnoreSpace) +
                String.format("(?:%s)*", encodingParams) +
                base64Pattern +
                String.format(beginOrEndFormat, "END", nameIgnoreSpace);
            pattern = Pattern.compile(patternStr);
        }

        private List<byte[]> pemEntries(String pem) {
            Matcher matcher = pattern.matcher(pem + "\n"); // allow last newline to be omitted in value
            List<byte[]>  entries = new ArrayList<>();
            while (matcher.find()) {
                String base64Str = matcher.group(1).replaceAll("\\s", "");
                entries.add(Base64.getDecoder().decode(base64Str));
            }
            if (entries.isEmpty())
                throw new InvalidConfigurationException("No matching " + name + " entries in PEM file");
            return entries;
        }
    }
}
