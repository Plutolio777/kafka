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
package org.apache.kafka.common.metrics;

import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * Sensor 关联一组Metrics
 * 传感器类用于对一组关联的指标应用连续的数值序列。例如，用于监控消息大小的传感器，
 * 将使用 {@link #record(double)} API 记录一系列的消息大小，并维护关于请求大小的指标集，
 * 如平均值或最大值。
 */

public final class Sensor {

    // mark 注册在传感器上的Metrics集合
    private final Metrics registry;
    // mark 传感器名称
    private final String name;
    // mark 传感器的父传感器
    private final Sensor[] parents;

    private final List<StatAndConfig> stats;
    // mark 关联指标名称以及实例的映射表
    private final Map<MetricName, KafkaMetric> metrics;
    // mark 指标配置
    private final MetricConfig config;
    // mark 时间工具类
    private final Time time;
    // mark 最后注册时间
    private volatile long lastRecordTime;
    // mark 传感器未活动过期时间
    private final long inactiveSensorExpirationTimeMs;
    // mark 指标对象锁
    private final Object metricLock;

    private static class StatAndConfig {
        private final Stat stat;
        private final Supplier<MetricConfig> configSupplier;

        StatAndConfig(Stat stat, Supplier<MetricConfig> configSupplier) {
            this.stat = stat;
            this.configSupplier = configSupplier;
        }

        public Stat stat() {
            return stat;
        }

        public MetricConfig config() {
            return configSupplier.get();
        }

        @Override
        public String toString() {
            return "StatAndConfig(stat=" + stat + ')';
        }
    }

    public enum RecordingLevel {
        INFO(0, "INFO"), DEBUG(1, "DEBUG"), TRACE(2, "TRACE");

        private static final RecordingLevel[] ID_TO_TYPE;
        private static final int MIN_RECORDING_LEVEL_KEY = 0;
        public static final int MAX_RECORDING_LEVEL_KEY;

        static {
            int maxRL = -1;
            for (RecordingLevel level : RecordingLevel.values()) {
                maxRL = Math.max(maxRL, level.id);
            }
            RecordingLevel[] idToName = new RecordingLevel[maxRL + 1];
            for (RecordingLevel level : RecordingLevel.values()) {
                idToName[level.id] = level;
            }
            ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final short id;

        RecordingLevel(int id, String name) {
            this.id = (short) id;
            this.name = name;
        }

        public static RecordingLevel forId(int id) {
            if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
                throw new IllegalArgumentException(String.format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
                    "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
            return ID_TO_TYPE[id];
        }

        /** Case insensitive lookup by protocol name */
        public static RecordingLevel forName(String name) {
            return RecordingLevel.valueOf(name.toUpperCase(Locale.ROOT));
        }

        public boolean shouldRecord(final int configId) {
            if (configId == INFO.id) {
                return this.id == INFO.id;
            } else if (configId == DEBUG.id) {
                return this.id == INFO.id || this.id == DEBUG.id;
            } else if (configId == TRACE.id) {
                return true;
            } else {
                throw new IllegalStateException("Did not recognize recording level " + configId);
            }
        }
    }

    private final RecordingLevel recordingLevel;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time,
           long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel) {
        super();
        // mark Metrics 注册表
        this.registry = registry;
        this.name = Objects.requireNonNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new LinkedHashMap<>();
        this.stats = new ArrayList<>();
        this.config = config;
        this.time = time;
        this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
        this.lastRecordTime = time.milliseconds();
        this.recordingLevel = recordingLevel;
        this.metricLock = new Object();
        checkForest(new HashSet<>());
    }

    /* Validate that this sensor doesn't end up referencing itself */
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (Sensor parent : parents)
            parent.checkForest(sensors);
    }

    /**
     * The name this sensor is registered with. This name will be unique among all registered sensors.
     */
    public String name() {
        return this.name;
    }

    List<Sensor> parents() {
        return unmodifiableList(asList(parents));
    }

    /**
     * @return true if the sensor's record level indicates that the metric will be recorded, false otherwise
     */
    public boolean shouldRecord() {
        return this.recordingLevel.shouldRecord(config.recordLevel().id);
    }

    /**
     * 无参记录方法 默认值为1
     */
    public void record() {
        // 检查当前情况是否适合进行记录，如果适合则执行记录操作。
        if (shouldRecord()) {
            recordInternal(1.0d, time.milliseconds(), true);
        }
    }


    /**
     * 记录 value
     */
    public void record(double value) {
        if (shouldRecord()) {
            recordInternal(value, time.milliseconds(), true);
        }
    }

    /**
     * 无参记录方法 默认值为1 并且过期时间
     */
    public void record(double value, long timeMs) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, true);
        }
    }

    /**
     * 在指定时间记录一个值。
     * 与 {@link #record(double)} 方法相比，此方法在已知记录时间的情况下略快，因为它重用了时间戳，避免了重复计算当前时间，从而提高了性能。
     *
     * @param value 要记录的值，表示特定的测量或计数。
     * @param timeMs 当前的POSIX时间（以毫秒为单位）。
     * @param checkQuotas 指示是否需要执行配额限制。
     * @throws QuotaViolationException 如果记录此值导致某个指标超出其配置的最大或最小界限，则抛出此异常。
     */
    public void record(double value, long timeMs, boolean checkQuotas) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, checkQuotas);
        }
    }


    /**
     * 内部记录方法，用于在特定时间记录给定的值到指标。
     *
     * @param value       要记录的数值
     * @param timeMs      记录的时间戳（毫秒）
     * @param checkQuotas 是否检查配额
     *                    <p>
     *                    此方法首先更新最后一次记录的时间，然后同步实例和指标锁以确保线程安全地更新所有统计信息。
     *                    如果需要，会检查配额。最后，递归地为所有父级传感器调用record方法。
     */
    private void recordInternal(double value, long timeMs, boolean checkQuotas) {
        this.lastRecordTime = timeMs;
        synchronized (this) {
            synchronized (metricLock()) {
                // 增量更新所有统计信息
                for (StatAndConfig statAndConfig : this.stats) {
                    statAndConfig.stat.record(statAndConfig.config(), value, timeMs);
                }
            }
            if (checkQuotas)
                checkQuotas(timeMs);
        }
        for (Sensor parent : parents)
            parent.record(value, timeMs, checkQuotas);
    }

    /**
     * Check if we have violated our quota for any metric that has a configured quota
     */
    public void checkQuotas() {
        checkQuotas(time.milliseconds());
    }

    public void checkQuotas(long timeMs) {
        for (KafkaMetric metric : this.metrics.values()) {
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota();
                if (quota != null) {
                    double value = metric.measurableValue(timeMs);
                    if (metric.measurable() instanceof TokenBucket) {
                        if (value < 0) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    } else {
                        if (!quota.acceptable(value)) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    }
                }
            }
        }
    }

    /**
     * 使用无配置覆盖为该传感器注册一个复合Metrics（CompoundStat）
     * @param stat 要注册的统计信息(Metrics)
     * @return 如果统计信息被添加到传感器，则返回 true；如果传感器已过期，则返回 false
     */
    public boolean add(CompoundStat stat) {
        return add(stat, null);
    }

    /**
     * 使用指定配置为该传感器注册一个复合统计信息(CompoundStat)，该统计信息生成多个可测量的数量（如直方图）
     * @param stat 要注册的统计信息
     * @param config 此统计信息的配置。如果为 null，则该统计信息将使用传感器的默认配置。
     * @return 如果统计信息被添加到传感器，则返回 true；如果传感器已过期，则返回 false
     */
    public synchronized boolean add(CompoundStat stat, MetricConfig config) {
        // mark 如果该传感器过期则返回false 注册失败
        if (hasExpired())
            return false;
        // mark 是否传入指定配置 如果没有则使用默认配置 默认配置参考 Metrics.config
        final MetricConfig statConfig = config == null ? this.config : config;

        // mark Stat与Config的包装类 主要是调用Stat.record()记录指标 Sensor.record会让关联的指标都记录一组数据
        stats.add(new StatAndConfig(Objects.requireNonNull(stat), () -> statConfig));
        Object lock = metricLock();
        for (NamedMeasurable m : stat.stats()) {
            final KafkaMetric metric = new KafkaMetric(lock, m.name(), m.stat(), statConfig, time);
            if (!metrics.containsKey(metric.metricName())) {
                KafkaMetric existingMetric = registry.registerMetric(metric);
                if (existingMetric != null) {
                    throw new IllegalArgumentException("A metric named '" + metric.metricName() + "' already exists, can't register another one.");
                }
                metrics.put(metric.metricName(), metric);
            }
        }
        return true;
    }

    /**
     * 为该传感器注册一个度量指标（MeasurableStat）
     * @param metricName 指标的名称
     * @param stat 要保留的统计信息
     * @return 如果指标被添加到传感器，则返回 true；如果传感器已过期，则返回 false
     */
    public boolean add(MetricName metricName, MeasurableStat stat) {
        return add(metricName, stat, null);
    }

    /**
     * 为该传感器注册一个度量指标（MeasurableStat）
     *
     * @param metricName 指标的名称
     * @param stat 要保留的统计信息
     * @param config 此指标的特殊配置。如果为 null，则使用传感器的默认配置。
     * @return 如果指标被添加到传感器，则返回 true；如果传感器已过期，则返回 false
     */
    public synchronized boolean add(final MetricName metricName, final MeasurableStat stat, final MetricConfig config) {
        // mark 如果Sensor过期 则返回false 注册失败
        if (hasExpired()) {
            return false;
        // mark 指标已经注册了则直接放回true
        } else if (metrics.containsKey(metricName)) {
            return true;
        } else {
            // mark 是否有传入MetricConfig用于覆盖默认的配置
            final MetricConfig statConfig = config == null ? this.config : config;
            // mark 生成kafkaMetric 这个是一个统一的包装类
            final KafkaMetric metric = new KafkaMetric(
                metricLock(),
                Objects.requireNonNull(metricName),
                Objects.requireNonNull(stat),
                    statConfig,
                time
            );
            // mark 在注册表中注册该Metric
            KafkaMetric existingMetric = registry.registerMetric(metric);

            // mark 重复注册抛出异常
            if (existingMetric != null) {
                throw new IllegalArgumentException("A metric named '" + metricName + "' already exists, can't register another one.");
            }
            // mark 添加到自己的缓存中
            metrics.put(metric.metricName(), metric);
            // mark 添加stats记录
            stats.add(new StatAndConfig(Objects.requireNonNull(stat), metric::config));
            return true;
        }
    }

    /**
     * Return if metrics were registered with this sensor.
     *
     * @return true if metrics were registered, false otherwise
     */
    public synchronized boolean hasMetrics() {
        return !metrics.isEmpty();
    }

    /**
     * 如果传感器由于不活动而符合移除条件，则返回 true；否则返回 false。
     * 当前时间 - 最后注册时间 大于 inactiveSensorExpirationTimeMs 则为过期
     */
    public boolean hasExpired() {
        return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
    }

    synchronized List<KafkaMetric> metrics() {
        return unmodifiableList(new ArrayList<>(this.metrics.values()));
    }

    /**
     * KafkaMetrics of sensors which use SampledStat should be synchronized on the same lock
     * for sensor record and metric value read to allow concurrent reads and updates. For simplicity,
     * all sensors are synchronized on this object.
     * <p>
     * Sensor object is not used as a lock for reading metric value since metrics reporter is
     * invoked while holding Sensor and Metrics locks to report addition and removal of metrics
     * and synchronized reporters may deadlock if Sensor lock is used for reading metrics values.
     * Note that Sensor object itself is used as a lock to protect the access to stats and metrics
     * while recording metric values, adding and deleting sensors.
     * </p><p>
     * Locking order (assume all MetricsReporter methods may be synchronized):
     * <ul>
     *   <li>Sensor#add: Sensor -> Metrics -> MetricsReporter</li>
     *   <li>Metrics#removeSensor: Sensor -> Metrics -> MetricsReporter</li>
     *   <li>KafkaMetric#metricValue: MetricsReporter -> Sensor#metricLock</li>
     *   <li>Sensor#record: Sensor -> Sensor#metricLock</li>
     * </ul>
     * </p>
     */
    private Object metricLock() {
        return metricLock;
    }
}
