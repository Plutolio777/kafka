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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.internals.MetricsUtils;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * 传感器和指标的注册表。
 * <p>
 * 指标是具名的数值测量。传感器是记录数值测量事件发生的句柄。每个传感器可以关联零个或多个指标。例如，一个传感器可以表示消息大小，
 * 我们可以为该传感器关联平均值、最大值或其他统计数据的指标，这些统计数据是基于传感器记录的消息大小序列计算得出的。
 * <p>
 * 使用方法如下：
 *
 * <pre>
 * // 设置指标：
 * Metrics metrics = new Metrics(); // 这是全局的指标和传感器存储库
 * Sensor sensor = metrics.sensor("message-sizes");
 * MetricName metricName = new MetricName("message-size-avg", "producer-metrics");
 * sensor.add(metricName, new Avg());
 * metricName = new MetricName("message-size-max", "producer-metrics");
 * sensor.add(metricName, new Max());
 *
 * // 随着消息发送，记录消息大小
 * sensor.record(messageSize);
 * </pre>
 */
public class Metrics implements Closeable {

    // mark 保存metric配置的实例，用于配置metric的行为和属性
    private final MetricConfig config;

    // mark 用于存储所有的指标实例
    private final ConcurrentMap<MetricName, KafkaMetric> metrics;

    // mark 存储所有的Sensor
    private final ConcurrentMap<String, Sensor> sensors;

    // 用于存储传感器的子传感器列表的并发映射，一个sensor可以有多个子sensor，这个映射用于管理这些关系
    private final ConcurrentMap<Sensor, List<Sensor>> childrenSensors;

    // 保存所有metrics reporter的列表，Metrics reporter负责将收集到的metrics报告给指定的reporting目标
    private final List<MetricsReporter> reporters;

    // 提供时间操作的抽象，用于metrics中需要时间操作的地方
    private final Time time;

    // 用于调度metrics任务的线程池执行器，它负责按照预定的时间间隔执行metrics的收集和报告任务
    private final ScheduledThreadPoolExecutor metricsScheduler;

    // 日志记录器实例，用于记录Metrics类的操作日志
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);


    /**
     * Create a metrics repository with no metric reporters and default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics() {
        this(new MetricConfig());
    }

    /**
     * Create a metrics repository with no metric reporters and default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics(Time time) {
        this(new MetricConfig(), new ArrayList<>(0), time);
    }

    /**
     * Create a metrics repository with no metric reporters and the given default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics(MetricConfig defaultConfig, Time time) {
        this(defaultConfig, new ArrayList<>(0), time);
    }


  /**
     * Create a metrics repository with no reporters and the given default config. This config will be used for any
     * metric that doesn't override its own config. Expiration of Sensors is disabled.
     * @param defaultConfig The default config to use for all metrics that don't override their config
     */
    public Metrics(MetricConfig defaultConfig) {
        this(defaultConfig, new ArrayList<>(0), Time.SYSTEM);
    }

    /**
     * Create a metrics repository with a default config and the given metric reporters.
     * Expiration of Sensors is disabled.
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time) {
        this(defaultConfig, reporters, time, false);
    }

    /**
     * Create a metrics repository with a default config, metric reporters and metric context
     * Expiration of Sensors is disabled.
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param metricsContext The metricsContext to initialize metrics reporter with
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, MetricsContext metricsContext) {
        this(defaultConfig, reporters, time, false, metricsContext);
    }

    /**
     * Create a metrics repository with a default config, given metric reporters and the ability to expire eligible sensors
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param enableExpiration true if the metrics instance can garbage collect inactive sensors, false otherwise
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, boolean enableExpiration) {
        this(defaultConfig, reporters, time, enableExpiration, new KafkaMetricsContext(""));
    }

    /**
     * Create a metrics repository with a default config, given metric reporters, the ability to expire eligible sensors
     * and MetricContext
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param enableExpiration true if the metrics instance can garbage collect inactive sensors, false otherwise
     * @param metricsContext The metricsContext to initialize metrics reporter with
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, boolean enableExpiration,
                   MetricsContext metricsContext) {
        // mark 默认指标配置
        this.config = defaultConfig;
        // mark 存储所有的Sensor
        this.sensors = new ConcurrentHashMap<>();
        // mark 存储所有的指标
        this.metrics = new ConcurrentHashMap<>();
        // mark 存储所有的子Sensor
        this.childrenSensors = new ConcurrentHashMap<>();
        // mark 存储所有的指标报告器
        this.reporters = Objects.requireNonNull(reporters);
        // mark 时间工具类
        this.time = time;

        // mark 重新改变指标报告器的上下文以及进行初始化
        for (MetricsReporter reporter : reporters) {
            reporter.contextChange(metricsContext);
            reporter.init(new ArrayList<>());
        }

        // Create the ThreadPoolExecutor only if expiration of Sensors is enabled.

        if (enableExpiration) {
            // mark 床架指标处理调度线程池
            this.metricsScheduler = new ScheduledThreadPoolExecutor(1);
            // Creating a daemon thread to not block shutdown
            // mark 配置线程工厂
            this.metricsScheduler.setThreadFactory(runnable -> KafkaThread.daemon("SensorExpiryThread", runnable));
            // mark 添加过去Sensor处理任务
            this.metricsScheduler.scheduleAtFixedRate(new ExpireSensorTask(), 30, 30, TimeUnit.SECONDS);
        } else {
            this.metricsScheduler = null;
        }

        addMetric(metricName("count", "kafka-metrics-count", "total number of registered metrics"),
            (config, now) -> metrics.size());
    }

    /**
     * 使用给定的名称、组名、描述和标签创建一个 MetricName，同时包含指标配置中指定的默认标签。
     * 如果在默认指标配置和提供的标签中有相同的标签键，则以提供的标签为准。
     *
     * @param name 指标的名称
     * @param group 指标所属的逻辑组名称
     * @param description 指标的人类可读描述
     * @param tags 指标的额外键值属性
     */
    public MetricName metricName(String name, String group, String description, Map<String, String> tags) {
        Map<String, String> combinedTag = new LinkedHashMap<>(config.tags());
        combinedTag.putAll(tags);
        return new MetricName(name, group, description, combinedTag);
    }

    /**
     * 使用给定的名称、组名、描述和指标配置中指定的默认标签创建一个 MetricName。
     *
     * @param name 指标的名称
     * @param group 指标所属的逻辑组名称
     * @param description 指标的人类可读描述
     */
    public MetricName metricName(String name, String group, String description) {
        return metricName(name, group, description, new HashMap<>());
    }

    /**
     * 使用给定的名称、组名以及指标配置中指定的默认标签创建一个 MetricName。
     *
     * @param name 指标的名称
     * @param group 指标所属的逻辑组名称
     */
    public MetricName metricName(String name, String group) {
        return metricName(name, group, "", new HashMap<>());
    }

    /**
     * 使用给定的名称、组名、描述以及键值对作为标签创建一个 MetricName，同时包含指标配置中指定的默认标签。
     * 如果在默认指标配置和提供的标签中有相同的标签键，则以提供的标签为准。
     *
     * @param name 指标的名称
     * @param group 指标所属的逻辑组名称
     * @param description 指标的人类可读描述
     * @param keyValue 指标的额外键值属性（必须成对出现）
     */
    public MetricName metricName(String name, String group, String description, String... keyValue) {
        return metricName(name, group, description, MetricsUtils.getTags(keyValue));
    }

    /**
     * 使用给定的名称、组名和标签创建一个 MetricName，同时包含指标配置中指定的默认标签。
     * 如果在默认指标配置和提供的标签中有相同的标签键，则以提供的标签为准。
     *
     * @param name 指标的名称
     * @param group 指标所属的逻辑组名称
     * @param tags 指标的键值属性
     */
    public MetricName metricName(String name, String group, Map<String, String> tags) {
        return metricName(name, group, "", tags);
    }

    /**
     * Use the specified domain and metric name templates to generate an HTML table documenting the metrics. A separate table section
     * will be generated for each of the MBeans and the associated attributes. The MBean names are lexicographically sorted to
     * determine the order of these sections. This order is therefore dependent upon the order of the
     * tags in each {@link MetricNameTemplate}.
     *
     * @param domain the domain or prefix for the JMX MBean names; may not be null
     * @param allMetrics the collection of all {@link MetricNameTemplate} instances each describing one metric; may not be null
     * @return the string containing the HTML table; never null
     */
    public static String toHtmlTable(String domain, Iterable<MetricNameTemplate> allMetrics) {
        Map<String, Map<String, String>> beansAndAttributes = new TreeMap<>();
    
        try (Metrics metrics = new Metrics()) {
            for (MetricNameTemplate template : allMetrics) {
                Map<String, String> tags = new LinkedHashMap<>();
                for (String s : template.tags()) {
                    tags.put(s, "{" + s + "}");
                }
    
                MetricName metricName = metrics.metricName(template.name(), template.group(), template.description(), tags);
                String mBeanName = JmxReporter.getMBeanName(domain, metricName);
                if (!beansAndAttributes.containsKey(mBeanName)) {
                    beansAndAttributes.put(mBeanName, new TreeMap<>());
                }
                Map<String, String> attrAndDesc = beansAndAttributes.get(mBeanName);
                if (!attrAndDesc.containsKey(template.name())) {
                    attrAndDesc.put(template.name(), template.description());
                } else {
                    throw new IllegalArgumentException("mBean '" + mBeanName + "' attribute '" + template.name() + "' is defined twice.");
                }
            }
        }
        
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
    
        for (Entry<String, Map<String, String>> e : beansAndAttributes.entrySet()) {
            b.append("<tr>\n");
            b.append("<td colspan=3 class=\"mbeanName\" style=\"background-color:#ccc; font-weight: bold;\">");
            b.append(e.getKey());
            b.append("</td>");
            b.append("</tr>\n");
            
            b.append("<tr>\n");
            b.append("<th style=\"width: 90px\"></th>\n");
            b.append("<th>Attribute name</th>\n");
            b.append("<th>Description</th>\n");
            b.append("</tr>\n");
            
            for (Entry<String, String> e2 : e.getValue().entrySet()) {
                b.append("<tr>\n");
                b.append("<td></td>");
                b.append("<td>");
                b.append(e2.getKey());
                b.append("</td>");
                b.append("<td>");
                b.append(e2.getValue());
                b.append("</td>");
                b.append("</tr>\n");
            }
    
        }
        b.append("</tbody></table>");
    
        return b.toString();
    
    }

    public MetricConfig config() {
        return config;
    }

    /**
     * Get the sensor with the given name if it exists
     * @param name The name of the sensor
     * @return Return the sensor or null if no such sensor exists
     */
    public Sensor getSensor(String name) {
        return this.sensors.get(Objects.requireNonNull(name));
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors. This uses
     * a default recording level of INFO.
     * @param name The sensor name
     * @return The sensor
     */
    public Sensor sensor(String name) {
        return this.sensor(name, Sensor.RecordingLevel.INFO);
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors and with a given
     * recording level.
     * @param name The sensor name.
     * @param recordingLevel The recording level.
     * @return The sensor
     */
    public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel) {
        return sensor(name, null, recordingLevel, (Sensor[]) null);
    }


    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public Sensor sensor(String name, Sensor... parents) {
        return this.sensor(name, Sensor.RecordingLevel.INFO, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor.
     * @param parents The parent sensors.
     * @param recordingLevel The recording level.
     * @return The sensor that is created
     */
    public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, null, recordingLevel, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, Sensor... parents) {
        return this.sensor(name, config, Sensor.RecordingLevel.INFO, parents);
    }


    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param recordingLevel The recording level.
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, config, Long.MAX_VALUE, recordingLevel, parents);
    }

    /**
     * 获取或创建一个具有给定唯一名称和零个或多个父传感器的传感器。所有父传感器将接收此传感器记录的每个值。
     *
     * @param name 传感器的名称
     * @param config 用于此传感器的默认配置，对于没有自己配置的指标
     * @param inactiveSensorExpirationTimeSeconds 如果在此传感器上未记录任何值的持续时间，则它有资格被移除
     * @param parents 父传感器列表
     * @param recordingLevel 记录级别
     * @return 创建的传感器对象
     */
    public synchronized Sensor sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        // mark 尝试获取已经存在的sensor
        Sensor s = getSensor(name);
        if (s == null) {
            // mark 创建一个新的sensor
            s = new Sensor(this, name, parents, config == null ? this.config : config, time, inactiveSensorExpirationTimeSeconds, recordingLevel);
            // mark 注册sensor
            this.sensors.put(name, s);
            // mark 如果父传感器不为空则继续添加到字Sensor注册表
            if (parents != null) {
                for (Sensor parent : parents) {
                    List<Sensor> children = childrenSensors.computeIfAbsent(parent, k -> new ArrayList<>());
                    children.add(s);
                }
            }
            log.trace("Added sensor with name {}", name);
        }
        return s;
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param inactiveSensorExpirationTimeSeconds If no value if recorded on the Sensor for this duration of time,
     *                                        it is eligible for removal
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor... parents) {
        return this.sensor(name, config, inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel.INFO, parents);
    }

    /**
     * 删除指定的传感器（如果存在），以及与之相关的指标和子传感器。
     * 如果该传感器不存在，则不执行任何操作。
     *
     * @param name 要删除的传感器的名称。
     */
    public void removeSensor(String name) {
        // mark 获取传感器实例
        Sensor sensor = sensors.get(name);
        if (sensor != null) {
            // 同步处理以确保线程安全
            List<Sensor> childSensors = null;
            synchronized (sensor) {
                synchronized (this) {
                    // mark 从传感器集合中移除传感器
                    if (sensors.remove(name, sensor)) {
                        // mark 移除与传感器关联的所有指标
                        for (KafkaMetric metric : sensor.metrics())
                            removeMetric(metric.metricName());
                        // mark 记录日志，指示已移除的传感器名称
                        log.trace("Removed sensor with name {}", name);
                        // mark 从子传感器集合中移除当前传感器的子传感器
                        childSensors = childrenSensors.remove(sensor);
                        // mark 遍历并更新父传感器的子传感器列表
                        for (final Sensor parent : sensor.parents()) {
                            childrenSensors.getOrDefault(parent, emptyList()).remove(sensor);
                        }
                    }
                }
            }
            // mark 递归移除子传感器
            if (childSensors != null) {
                for (Sensor childSensor : childSensors)
                    removeSensor(childSensor.name());
            }
        }
    }


    /**
     * Add a metric to monitor an object that implements measurable. This metric won't be associated with any sensor.
     * This is a way to expose existing values as metrics.
     *
     * This method is kept for binary compatibility purposes, it has the same behaviour as
     * {@link #addMetric(MetricName, MetricValueProvider)}.
     *
     * @param metricName The name of the metric
     * @param measurable The measurable that will be measured by this metric
     */
    public void addMetric(MetricName metricName, Measurable measurable) {
        addMetric(metricName, null, measurable);
    }

    /**
     * Add a metric to monitor an object that implements Measurable. This metric won't be associated with any sensor.
     * This is a way to expose existing values as metrics.
     *
     * This method is kept for binary compatibility purposes, it has the same behaviour as
     * {@link #addMetric(MetricName, MetricConfig, MetricValueProvider)}.
     *
     * @param metricName The name of the metric
     * @param config The configuration to use when measuring this measurable
     * @param measurable The measurable that will be measured by this metric
     */
    public void addMetric(MetricName metricName, MetricConfig config, Measurable measurable) {
        // mark 这里会把Measurable转换成MetricValueProvider
        addMetric(metricName, config, (MetricValueProvider<?>) measurable);
    }

    /**
     * 将一个实现了 MetricValueProvider 接口的对象添加为监视的指标。此指标不会与任何传感器关联。
     * 这是一种将现有值作为指标公开的方式。如果需要，用户应添加任何额外的同步来更新和访问指标值。
     *
     * @param metricName 指标的名称
     * @param metricValueProvider 与此指标关联的指标值提供者
     * @throws IllegalArgumentException 如果同名的指标已经存在
     */
    public void addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        // mark 创建KafkaMetric
        KafkaMetric m = new KafkaMetric(new Object(),
                                        Objects.requireNonNull(metricName),
                                        Objects.requireNonNull(metricValueProvider),
                                        config == null ? this.config : config,
                                        time);
        // mark 注册指标
        KafkaMetric existingMetric = registerMetric(m);
        // mark 指标重复注册的时候抛出异常
        if (existingMetric != null) {
            throw new IllegalArgumentException("A metric named '" + metricName + "' already exists, can't register another one.");
        }
    }

    /**
     * Add a metric to monitor an object that implements MetricValueProvider. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics. User is expected to add any additional
     * synchronization to update and access metric values, if required.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     */
    public void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider) {
        addMetric(metricName, null, metricValueProvider);
    }

    /**
     * Create or get an existing metric to monitor an object that implements MetricValueProvider.
     * This metric won't be associated with any sensor. This is a way to expose existing values as metrics.
     * This method takes care of synchronisation while updating/accessing metrics by concurrent threads.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     * @return Existing KafkaMetric if already registered or else a newly created one
     */
    public KafkaMetric addMetricIfAbsent(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        KafkaMetric metric = new KafkaMetric(new Object(),
                Objects.requireNonNull(metricName),
                Objects.requireNonNull(metricValueProvider),
                config == null ? this.config : config,
                time);

        KafkaMetric existingMetric = registerMetric(metric);
        return existingMetric == null ? metric : existingMetric;
    }

    /**
     * Remove a metric if it exists and return it. Return null otherwise. If a metric is removed, `metricRemoval`
     * will be invoked for each reporter.
     *
     * @param metricName The name of the metric
     * @return the removed `KafkaMetric` or null if no such metric exists
     */
    public synchronized KafkaMetric removeMetric(MetricName metricName) {
        KafkaMetric metric = this.metrics.remove(metricName);
        if (metric != null) {
            for (MetricsReporter reporter : reporters) {
                try {
                    reporter.metricRemoval(metric);
                } catch (Exception e) {
                    log.error("Error when removing metric from " + reporter.getClass().getName(), e);
                }
            }
            log.trace("Removed metric named {}", metricName);
        }
        return metric;
    }

    /**
     * Add a MetricReporter
     */
    public synchronized void addReporter(MetricsReporter reporter) {
        Objects.requireNonNull(reporter).init(new ArrayList<>(metrics.values()));
        this.reporters.add(reporter);
    }

    /**
     * Remove a MetricReporter
     */
    public synchronized void removeReporter(MetricsReporter reporter) {
        if (this.reporters.remove(reporter)) {
            reporter.close();
        }
    }

    /**
     * 注册一个指标，如果不存在则注册，如果已存在则返回具有相同名称的已存在指标。
     * 当一个指标被新注册时，该方法返回 null。
     *
     * @param metric 要注册的 KafkaMetric
     * @return 具有相同名称的已存在指标或 null
     */
    synchronized KafkaMetric registerMetric(KafkaMetric metric) {
        // mark 将MetricName作为key注册到map中
        MetricName metricName = metric.metricName();
        KafkaMetric existingMetric = this.metrics.putIfAbsent(metricName, metric);
        if (existingMetric != null) {
            return existingMetric;
        }
        // newly added metric
        // mark 指标报告器中通知并注册指标
        for (MetricsReporter reporter : reporters) {
            try {
                reporter.metricChange(metric);
            } catch (Exception e) {
                log.error("Error when registering metric on " + reporter.getClass().getName(), e);
            }
        }
        log.trace("Registered metric named {}", metricName);
        return null;
    }

    /**
     * Get all the metrics currently maintained indexed by metricName
     */
    public Map<MetricName, KafkaMetric> metrics() {
        return this.metrics;
    }

    public List<MetricsReporter> reporters() {
        return this.reporters;
    }

    public KafkaMetric metric(MetricName metricName) {
        return this.metrics.get(metricName);
    }

    /**
     * ExpireSensorTask 类实现了 Runnable 接口，其职责是检查并移除已过期的传感器。
     * 此类为包私有，主要用于测试目的。
     */
    class ExpireSensorTask implements Runnable {
        @Override
        public void run() {
            // mark 遍历传感器映射，检查每个传感器是否已过期
            for (Map.Entry<String, Sensor> sensorEntry : sensors.entrySet()) {
                // mark 对传感器对象进行同步，以确保在检查和移除传感器时的线程安全
                synchronized (sensorEntry.getValue()) {
                    // mark 如果传感器已过期，记录信息并移除传感器
                    if (sensorEntry.getValue().hasExpired()) {
                        log.debug("移除已过期的传感器 {}", sensorEntry.getKey());
                        removeSensor(sensorEntry.getKey());
                    }
                }
            }
        }
    }


    /* For testing use only. */
    Map<Sensor, List<Sensor>> childrenSensors() {
        return Collections.unmodifiableMap(childrenSensors);
    }

    public MetricName metricInstance(MetricNameTemplate template, String... keyValue) {
        return metricInstance(template, MetricsUtils.getTags(keyValue));
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        // check to make sure that the runtime defined tags contain all the template tags.
        Set<String> runtimeTagKeys = new HashSet<>(tags.keySet());
        runtimeTagKeys.addAll(config().tags().keySet());
        
        Set<String> templateTagKeys = template.tags();
        
        if (!runtimeTagKeys.equals(templateTagKeys)) {
            throw new IllegalArgumentException("For '" + template.name() + "', runtime-defined metric tags do not match the tags in the template. "
                    + "Runtime = " + runtimeTagKeys.toString() + " Template = " + templateTagKeys.toString());
        }
                
        return this.metricName(template.name(), template.group(), template.description(), tags);
    }

    /**
     * Close this metrics repository.
     */
    @Override
    public void close() {
        if (this.metricsScheduler != null) {
            this.metricsScheduler.shutdown();
            try {
                this.metricsScheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // ignore and continue shutdown
                Thread.currentThread().interrupt();
            }
        }
        log.info("Metrics scheduler closed");

        for (MetricsReporter reporter : reporters) {
            try {
                log.info("Closing reporter {}", reporter.getClass().getName());
                reporter.close();
            } catch (Exception e) {
                log.error("Error when closing " + reporter.getClass().getName(), e);
            }
        }
        log.info("Metrics reporters closed");
    }
}
