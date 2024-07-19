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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Time;

public final class KafkaMetric implements Metric {

    // mark 指标名称
    private MetricName metricName;
    // mark 对象锁
    private final Object lock;
    // mark 时间操作类
    private final Time time;
    // mark 指标提供器
    private final MetricValueProvider<?> metricValueProvider;
    // mark 指标配置
    private MetricConfig config;

    // public for testing
    public KafkaMetric(Object lock, MetricName metricName, MetricValueProvider<?> valueProvider,
            MetricConfig config, Time time) {
        this.metricName = metricName;
        this.lock = lock;
        // mark 指标提供器必须是 Measurable或者Gauge
        if (!(valueProvider instanceof Measurable) && !(valueProvider instanceof Gauge))
            throw new IllegalArgumentException("Unsupported metric value provider of class " + valueProvider.getClass());
        this.metricValueProvider = valueProvider;
        this.config = config;
        this.time = time;
    }

    public MetricConfig config() {
        return this.config;
    }

    @Override
    public MetricName metricName() {
        return this.metricName;
    }


    /**
     * 获取当前指标的值。
     * 根据当前时间和配置，使用同步锁确保安全访问。
     * 如果 metricValueProvider 是 Measurable 的实例，则调用其 measure 方法获取指标值。
     * 如果 metricValueProvider 是 Gauge 的实例，则调用其 value 方法获取指标值。
     * 否则，抛出 IllegalStateException 异常，指示 metricValueProvider 不是有效的指标提供者。
     *
     * @return 当前指标的值
     * @throws IllegalStateException 如果 metricValueProvider 不是有效的指标提供者
     */
    @Override
    public Object metricValue() {
        long now = time.milliseconds();
        // mark 锁住这个指标方法
        synchronized (this.lock) {
            // mark 如果 metricValueProvider 是 Measurable 的实例，则调用其 measure 方法获取指标值。
            if (this.metricValueProvider instanceof Measurable)
                return ((Measurable) metricValueProvider).measure(config, now);
                // mark 如果 metricValueProvider 是 Gauge 的实例，则调用其 value 方法获取指标值。
            else if (this.metricValueProvider instanceof Gauge)
                return ((Gauge<?>) metricValueProvider).value(config, now);
            else
                throw new IllegalStateException("Not a valid metric: " + this.metricValueProvider.getClass());
        }
    }

    public Measurable measurable() {
        if (this.metricValueProvider instanceof Measurable)
            return (Measurable) metricValueProvider;
        else
            throw new IllegalStateException("Not a measurable: " + this.metricValueProvider.getClass());
    }

    double measurableValue(long timeMs) {
        synchronized (this.lock) {
            if (this.metricValueProvider instanceof Measurable)
                return ((Measurable) metricValueProvider).measure(config, timeMs);
            else
                return 0;
        }
    }

    public void config(MetricConfig config) {
        synchronized (lock) {
            this.config = config;
        }
    }
}
