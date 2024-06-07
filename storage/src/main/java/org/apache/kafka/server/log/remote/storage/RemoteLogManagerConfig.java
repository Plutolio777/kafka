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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * 提供了一系列配置选项，以控制如何将 Kafka 的日志段存储在远程存储系统中。
 */
public final class RemoteLogManagerConfig {

    /**
     * Prefix used for properties to be passed to {@link RemoteStorageManager} implementation. Remote log subsystem collects all the properties having
     * this prefix and passes to {@code RemoteStorageManager} using {@link RemoteStorageManager#configure(Map)}.
     */
    public static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP = "remote.log.storage.manager.impl.prefix";
    public static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_DOC = "Prefix used for properties to be passed to RemoteStorageManager " +
            "implementation. For example this value can be `rsm.s3.`.";

    /**
     * Prefix used for properties to be passed to {@link RemoteLogMetadataManager} implementation. Remote log subsystem collects all the properties having
     * this prefix and passed to {@code RemoteLogMetadataManager} using {@link RemoteLogMetadataManager#configure(Map)}.
     */
    public static final String REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP = "remote.log.metadata.manager.impl.prefix";
    public static final String REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_DOC = "Prefix used for properties to be passed to RemoteLogMetadataManager " +
            "implementation. For example this value can be `rlmm.s3.`.";

    /**
     * remote.log.storage.system.enable=true
     * 设置为 true 表示是否启用远程日志存储系统。
     * 默认值 false
     */
    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP = "remote.log.storage.system.enable";
    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE_DOC = "Whether to enable tier storage functionality in a broker or not. Valid values " +
            "are `true` or `false` and the default value is false. When it is true broker starts all the services required for tiered storage functionality.";
    public static final boolean DEFAULT_REMOTE_LOG_STORAGE_SYSTEM_ENABLE = false;

    /**
     * remote.log.storage.manager.class.name
     * 用于指定实现 RemoteLogManager 接口的类的全限定名。
     * 这个类负责管理 Kafka 的远程日志存储操作，包括上传、删除和检索日志段。
     * 这个类需要实现 {@link RemoteStorageManager} 接口，并定义具体的远程存储逻辑
     * 默认值 null
     */
    public static final String REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP = "remote.log.storage.manager.class.name";
    public static final String REMOTE_STORAGE_MANAGER_CLASS_NAME_DOC = "Fully qualified class name of `RemoteLogStorageManager` implementation.";

    /**
     * remote.log.storage.manager.class.name
     * 于指定包含 {@link RemoteStorageManager} 实现类的路径。
     * 默认值 null
     */
    public static final String REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP = "remote.log.storage.manager.class.path";
    public static final String REMOTE_STORAGE_MANAGER_CLASS_PATH_DOC = "Class path of the `RemoteLogStorageManager` implementation." +
            "If specified, the RemoteLogStorageManager implementation and its dependent libraries will be loaded by a dedicated" +
            "classloader which searches this class path before the Kafka broker class path. The syntax of this parameter is same" +
            "with the standard Java class path string.";

    /**
     * remote.log.metadata.manager.class.name
     * 用于指定实现 RemoteLogMetadataManager 接口的类的全限定名。
     * 这个类负责存储和检索远程日志段的元数据，以确保远程日志存储系统的元数据管理具有一致性和可靠性。
     * 这个类需要实现 {@link RemoteLogMetadataManager} 接口，并定义具体的远程存储逻辑
     * 默认值 null
     */
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP = "remote.log.metadata.manager.class.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_DOC = "Fully qualified class name of `RemoteLogMetadataManager` implementation.";
    //todo add the default topic based RLMM class name.
    public static final String DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME = "";

    /**
     * remote.log.metadata.manager.class.path
     * 于指定包含 {@link RemoteLogMetadataManager} 实现类的路径。
     * 默认值 null
     */
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP = "remote.log.metadata.manager.class.path";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_DOC = "Class path of the `RemoteLogMetadataManager` implementation." +
            "If specified, the RemoteLogMetadataManager implementation and its dependent libraries will be loaded by a dedicated" +
            "classloader which searches this class path before the Kafka broker class path. The syntax of this parameter is same" +
            "with the standard Java class path string.";

    /**
     * remote.log.metadata.manager.listener.name
     * 用于指定用于连接 {@link RemoteLogMetadataManager} 的监听器名称。
     * 这个参数允许用户定义特定的监听器，用于远程日志元数据管理。
     * 默认值 null
     */
    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP = "remote.log.metadata.manager.listener.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_DOC = "Listener name of the local broker to which it should get connected if " +
            "needed by RemoteLogMetadataManager implementation.";

    /**
     * remote.log.index.file.cache.total.size.bytes
     * 指定本地缓存的总大小（以字节为单位），用于缓存从远程存储中检索到的日志段索引文件。
     * 通过配置这个参数，可以减少从远程存储频繁检索索引文件的次数，从而提高性能。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES}
     */
    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP = "remote.log.index.file.cache.total.size.bytes";
    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_DOC = "The total size of the space allocated to store index files fetched " +
            "from remote storage in the local storage.";
    public static final long DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES = 1024 * 1024 * 1024L;

    /**
     * remote.log.manager.thread.pool.size
     * 用于指定远程日志管理器的线程池大小。
     * 该参数决定了管理和执行远程日志存储任务（如上传、删除、缓存管理等）时可用的最大线程数。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE}
     */
    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP = "remote.log.manager.thread.pool.size";
    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_DOC = "Size of the thread pool used in scheduling tasks to copy " +
            "segments, fetch remote log indexes and clean up remote log segments.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE = 10;

    /**
     * remote.log.manager.task.interval.ms
     * 用于指定远程日志管理器任务的执行间隔时间（以毫秒为单位）
     * 该参数决定了远程日志管理器在多长时间间隔内执行其任务，例如上传和删除远程日志段。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS}
     */
    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP = "remote.log.manager.task.interval.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_DOC = "Interval at which remote log manager runs the scheduled tasks like copy " +
            "segments, and clean up remote log segments.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS = 30 * 1000L;

    /**
     * remote.log.manager.task.retry.backoff.ms
     * 用于指定远程日志管理器在任务失败后重试之前等待的时间（以毫秒为单位）。
     * 这个参数有助于控制任务重试的频率，避免过于频繁的重试操作导致系统性能下降。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS}
     */
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP = "remote.log.manager.task.retry.backoff.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_DOC = "The initial amount of wait in milli seconds before the request is retried again.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS = 500L;

    /**
     * remote.log.manager.task.retry.backoff.max.ms
     * 用于指定远程日志管理器在任务失败后重试之前等待的最大时间（以毫秒为单位）。
     * 这个参数有助于控制任务重试的频率，避免过于频繁的重试操作导致系统性能下降。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS}
     */
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP = "remote.log.manager.task.retry.backoff.max.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_DOC = "The maximum amount of time in milliseconds to wait when the request " +
            "is retried again. The retry duration will increase exponentially for each request failure up to this maximum wait interval.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS = 30 * 1000L;

    /**
     * remote.log.manager.task.retry.jitter
     * 用于在重试任务时添加抖动（jitter）。抖动可以防止在系统负载高的情况下，所有任务在同一时间进行重试，从而减少系统的突发压力。
     * 这个参数有助于控制任务重试的频率，避免过于频繁的重试操作导致系统性能下降。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS}
     */
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP = "remote.log.manager.task.retry.jitter";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_DOC = "The value used in defining the range for computing random jitter factor. " +
            "It is applied to the effective exponential term for computing the resultant retry backoff interval. This will avoid thundering herds " +
            "of requests. The default value is 0.2 and valid value should be between 0(inclusive) and 0.5(inclusive). " +
            "For ex: remote.log.manager.task.retry.jitter = 0.25, then the range to compute random jitter will be [1-0.25, 1+0.25) viz [0.75, 1.25). " +
            "So, jitter factor can be any random value with in that range.";
    public static final double DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_JITTER = 0.2;

    /**
     * remote.log.reader.threads
     * 用于指定用于读取远程日志的线程数。通过配置这个参数，可以控制并发读取远程日志段的线程数量，从而提高远程日志读取的效率。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_READER_THREADS}
     */
    public static final String REMOTE_LOG_READER_THREADS_PROP = "remote.log.reader.threads";
    public static final String REMOTE_LOG_READER_THREADS_DOC = "Size of the thread pool that is allocated for handling remote log reads.";
    public static final int DEFAULT_REMOTE_LOG_READER_THREADS = 10;

    /**
     * remote.log.reader.max.pending.tasks
     * 用于指定远程日志读取器线程池的最大任务队列大小。
     * 如果任务队列已满，则无法处理新的请求，并且会返回错误。
     * 默认值 {@link RemoteLogManagerConfig#DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS}
     */
    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP = "remote.log.reader.max.pending.tasks";
    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS_DOC = "Maximum remote log reader thread pool task queue size. If the task queue " +
            "is full, fetch requests are served with an error.";
    public static final int DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS = 100;

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    static {
        CONFIG_DEF.defineInternal(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP,
                                  BOOLEAN,
                                  DEFAULT_REMOTE_LOG_STORAGE_SYSTEM_ENABLE,
                                  null,
                                  MEDIUM,
                                  REMOTE_LOG_STORAGE_SYSTEM_ENABLE_DOC)
                  .defineInternal(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                                  STRING,
                                  null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_DOC)
                  .defineInternal(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP,
                                  STRING,
                                  null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_DOC)
                  .defineInternal(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, STRING,
                                  null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_STORAGE_MANAGER_CLASS_NAME_DOC)
                  .defineInternal(REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP, STRING,
                                  null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_STORAGE_MANAGER_CLASS_PATH_DOC)
                  .defineInternal(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                                  STRING, null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_DOC)
                  .defineInternal(REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP,
                                  STRING,
                                  null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_DOC)
                  .defineInternal(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, STRING,
                                  null,
                                  new ConfigDef.NonEmptyString(),
                                  MEDIUM,
                                  REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_DOC)
                  .defineInternal(REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
                                  LONG,
                                  DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES,
                                  atLeast(1),
                                  LOW,
                                  REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_DOC)
                  .defineInternal(REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP,
                                  INT,
                                  DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE,
                                  atLeast(1),
                                  MEDIUM,
                                  REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_DOC)
                  .defineInternal(REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP,
                                  LONG,
                                  DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS,
                                  atLeast(1),
                                  LOW,
                                  REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_DOC)
                  .defineInternal(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP,
                                  LONG,
                                  DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS,
                                  atLeast(1),
                                  LOW,
                                  REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_DOC)
                  .defineInternal(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP,
                                  LONG,
                                  DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS,
                                  atLeast(1), LOW,
                                  REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_DOC)
                  .defineInternal(REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP,
                                  DOUBLE,
                                  DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_JITTER,
                                  between(0, 0.5),
                                  LOW,
                                  REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_DOC)
                  .defineInternal(REMOTE_LOG_READER_THREADS_PROP,
                                  INT,
                                  DEFAULT_REMOTE_LOG_READER_THREADS,
                                  atLeast(1),
                                  MEDIUM,
                                  REMOTE_LOG_READER_THREADS_DOC)
                  .defineInternal(REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP,
                                  INT,
                                  DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS,
                                  atLeast(1),
                                  MEDIUM,
                                  REMOTE_LOG_READER_MAX_PENDING_TASKS_DOC);
    }

    private final boolean enableRemoteStorageSystem;
    private final String remoteStorageManagerClassName;
    private final String remoteStorageManagerClassPath;
    private final String remoteLogMetadataManagerClassName;
    private final String remoteLogMetadataManagerClassPath;
    private final long remoteLogIndexFileCacheTotalSizeBytes;
    private final int remoteLogManagerThreadPoolSize;
    private final long remoteLogManagerTaskIntervalMs;
    private final long remoteLogManagerTaskRetryBackoffMs;
    private final long remoteLogManagerTaskRetryBackoffMaxMs;
    private final double remoteLogManagerTaskRetryJitter;
    private final int remoteLogReaderThreads;
    private final int remoteLogReaderMaxPendingTasks;
    private final String remoteStorageManagerPrefix;
    private final HashMap<String, Object> remoteStorageManagerProps;
    private final String remoteLogMetadataManagerPrefix;
    private final HashMap<String, Object> remoteLogMetadataManagerProps;
    private final String remoteLogMetadataManagerListenerName;

    public RemoteLogManagerConfig(AbstractConfig config) {
        // mark 提取与远程日志管理相关的配置
        this(config.getBoolean(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP),
             config.getString(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP),
             config.getString(REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP),
             config.getLong(REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP),
             config.getInt(REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP),
             config.getDouble(REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP),
             config.getInt(REMOTE_LOG_READER_THREADS_PROP),
             config.getInt(REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP),
             config.getString(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP),
             config.getString(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP) != null
                 ? config.originalsWithPrefix(config.getString(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP))
                 : Collections.emptyMap(),
             config.getString(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP) != null
                 ? config.originalsWithPrefix(config.getString(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP))
                 : Collections.emptyMap());
    }

    // Visible for testing
    public RemoteLogManagerConfig(boolean enableRemoteStorageSystem,
                                  String remoteStorageManagerClassName,
                                  String remoteStorageManagerClassPath,
                                  String remoteLogMetadataManagerClassName,
                                  String remoteLogMetadataManagerClassPath,
                                  String remoteLogMetadataManagerListenerName,
                                  long remoteLogIndexFileCacheTotalSizeBytes,
                                  int remoteLogManagerThreadPoolSize,
                                  long remoteLogManagerTaskIntervalMs,
                                  long remoteLogManagerTaskRetryBackoffMs,
                                  long remoteLogManagerTaskRetryBackoffMaxMs,
                                  double remoteLogManagerTaskRetryJitter,
                                  int remoteLogReaderThreads,
                                  int remoteLogReaderMaxPendingTasks,
                                  String remoteStorageManagerPrefix,
                                  Map<String, Object> remoteStorageManagerProps, /* properties having keys stripped out with remoteStorageManagerPrefix */
                                  String remoteLogMetadataManagerPrefix,
                                  Map<String, Object> remoteLogMetadataManagerProps /* properties having keys stripped out with remoteLogMetadataManagerPrefix */
    ) {
        // mark 是否启用远程日志存储系统
        this.enableRemoteStorageSystem = enableRemoteStorageSystem;
        // mark 指定实现 RemoteLogManager 接口的类的全限定名。这个类负责管理 Kafka 的远程日志存储操作，包括上传、删除和检索日志段。
        this.remoteStorageManagerClassName = remoteStorageManagerClassName;
        //
        this.remoteStorageManagerClassPath = remoteStorageManagerClassPath;
        this.remoteLogMetadataManagerClassName = remoteLogMetadataManagerClassName;
        this.remoteLogMetadataManagerClassPath = remoteLogMetadataManagerClassPath;
        this.remoteLogIndexFileCacheTotalSizeBytes = remoteLogIndexFileCacheTotalSizeBytes;
        this.remoteLogManagerThreadPoolSize = remoteLogManagerThreadPoolSize;
        this.remoteLogManagerTaskIntervalMs = remoteLogManagerTaskIntervalMs;
        this.remoteLogManagerTaskRetryBackoffMs = remoteLogManagerTaskRetryBackoffMs;
        this.remoteLogManagerTaskRetryBackoffMaxMs = remoteLogManagerTaskRetryBackoffMaxMs;
        this.remoteLogManagerTaskRetryJitter = remoteLogManagerTaskRetryJitter;
        this.remoteLogReaderThreads = remoteLogReaderThreads;
        this.remoteLogReaderMaxPendingTasks = remoteLogReaderMaxPendingTasks;
        this.remoteStorageManagerPrefix = remoteStorageManagerPrefix;
        this.remoteStorageManagerProps = new HashMap<>(remoteStorageManagerProps);
        this.remoteLogMetadataManagerPrefix = remoteLogMetadataManagerPrefix;
        this.remoteLogMetadataManagerProps = new HashMap<>(remoteLogMetadataManagerProps);
        this.remoteLogMetadataManagerListenerName = remoteLogMetadataManagerListenerName;
    }

    public boolean enableRemoteStorageSystem() {
        return enableRemoteStorageSystem;
    }

    public String remoteStorageManagerClassName() {
        return remoteStorageManagerClassName;
    }

    public String remoteStorageManagerClassPath() {
        return remoteStorageManagerClassPath;
    }

    public String remoteLogMetadataManagerClassName() {
        return remoteLogMetadataManagerClassName;
    }

    public String remoteLogMetadataManagerClassPath() {
        return remoteLogMetadataManagerClassPath;
    }

    public long remoteLogIndexFileCacheTotalSizeBytes() {
        return remoteLogIndexFileCacheTotalSizeBytes;
    }

    public int remoteLogManagerThreadPoolSize() {
        return remoteLogManagerThreadPoolSize;
    }

    public long remoteLogManagerTaskIntervalMs() {
        return remoteLogManagerTaskIntervalMs;
    }

    public long remoteLogManagerTaskRetryBackoffMs() {
        return remoteLogManagerTaskRetryBackoffMs;
    }

    public long remoteLogManagerTaskRetryBackoffMaxMs() {
        return remoteLogManagerTaskRetryBackoffMaxMs;
    }

    public double remoteLogManagerTaskRetryJitter() {
        return remoteLogManagerTaskRetryJitter;
    }

    public int remoteLogReaderThreads() {
        return remoteLogReaderThreads;
    }

    public int remoteLogReaderMaxPendingTasks() {
        return remoteLogReaderMaxPendingTasks;
    }

    public String remoteLogMetadataManagerListenerName() {
        return remoteLogMetadataManagerListenerName;
    }

    public String remoteStorageManagerPrefix() {
        return remoteStorageManagerPrefix;
    }

    public String remoteLogMetadataManagerPrefix() {
        return remoteLogMetadataManagerPrefix;
    }

    public Map<String, Object> remoteStorageManagerProps() {
        return Collections.unmodifiableMap(remoteStorageManagerProps);
    }

    public Map<String, Object> remoteLogMetadataManagerProps() {
        return Collections.unmodifiableMap(remoteLogMetadataManagerProps);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemoteLogManagerConfig)) return false;
        RemoteLogManagerConfig that = (RemoteLogManagerConfig) o;
        return enableRemoteStorageSystem == that.enableRemoteStorageSystem
                && remoteLogIndexFileCacheTotalSizeBytes == that.remoteLogIndexFileCacheTotalSizeBytes
                && remoteLogManagerThreadPoolSize == that.remoteLogManagerThreadPoolSize
                && remoteLogManagerTaskIntervalMs == that.remoteLogManagerTaskIntervalMs
                && remoteLogManagerTaskRetryBackoffMs == that.remoteLogManagerTaskRetryBackoffMs
                && remoteLogManagerTaskRetryBackoffMaxMs == that.remoteLogManagerTaskRetryBackoffMaxMs
                && remoteLogManagerTaskRetryJitter == that.remoteLogManagerTaskRetryJitter
                && remoteLogReaderThreads == that.remoteLogReaderThreads
                && remoteLogReaderMaxPendingTasks == that.remoteLogReaderMaxPendingTasks
                && Objects.equals(remoteStorageManagerClassName, that.remoteStorageManagerClassName)
                && Objects.equals(remoteStorageManagerClassPath, that.remoteStorageManagerClassPath)
                && Objects.equals(remoteLogMetadataManagerClassName, that.remoteLogMetadataManagerClassName)
                && Objects.equals(remoteLogMetadataManagerClassPath, that.remoteLogMetadataManagerClassPath)
                && Objects.equals(remoteLogMetadataManagerListenerName, that.remoteLogMetadataManagerListenerName)
                && Objects.equals(remoteStorageManagerProps, that.remoteStorageManagerProps)
                && Objects.equals(remoteLogMetadataManagerProps, that.remoteLogMetadataManagerProps)
                && Objects.equals(remoteStorageManagerPrefix, that.remoteStorageManagerPrefix)
                && Objects.equals(remoteLogMetadataManagerPrefix, that.remoteLogMetadataManagerPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enableRemoteStorageSystem, remoteStorageManagerClassName, remoteStorageManagerClassPath,
                            remoteLogMetadataManagerClassName, remoteLogMetadataManagerClassPath, remoteLogMetadataManagerListenerName,
                            remoteLogIndexFileCacheTotalSizeBytes, remoteLogManagerThreadPoolSize, remoteLogManagerTaskIntervalMs,
                            remoteLogManagerTaskRetryBackoffMs, remoteLogManagerTaskRetryBackoffMaxMs, remoteLogManagerTaskRetryJitter,
                            remoteLogReaderThreads, remoteLogReaderMaxPendingTasks, remoteStorageManagerProps, remoteLogMetadataManagerProps,
                            remoteStorageManagerPrefix, remoteLogMetadataManagerPrefix);
    }
}
