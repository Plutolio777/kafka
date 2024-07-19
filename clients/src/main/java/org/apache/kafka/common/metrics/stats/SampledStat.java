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
package org.apache.kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * SampledStat 记录一个单一标量值，该值在一个或多个样本中测量。每个样本记录在可配置的窗口期内。窗口可以按事件数量或经过的时间定义（或两者兼有，如果两者都给定，则在达到任一事件计数或经过时间条件时窗口就完成）。
 * <p>
 * 所有样本被组合以生成测量结果。当窗口周期完成时，最旧的样本会被清除并重新利用，以开始记录下一个样本。
 * 该类的子类使用这种基本模式定义不同的统计数据。
 */
@SuppressWarnings("ClassEscapesDefinedScope")
public abstract class SampledStat implements MeasurableStat {

    private double initialValue;
    // mark 当前采样窗口
    private int current = 0;
    // mark 采样集合
    protected List<Sample> samples;

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        this.samples = new ArrayList<>(2);
    }


    /**
     * 记录指标值。
     *
     * @param config 指标的配置
     * @param value  要记录的值
     * @param timeMs 记录值的 POSIX 时间戳（毫秒）
     */
    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        // mark 获取当前的采样
        Sample sample = current(timeMs);
        // mark 如果当前采样已经完成
        if (sample.isComplete(timeMs, config))
            // mark  创建或者重设已有的Sample
            sample = advance(config, timeMs);
        // mark 更新采样值
        update(sample, config, value, timeMs);
        sample.eventCount += 1;
    }

    /**
     * 根据指定的配置和时间前进采样。
     *
     * @param config 指标的配置
     * @param timeMs POSIX 时间戳（毫秒）
     * @return 前进后的采样对象
     */
    private Sample advance(MetricConfig config, long timeMs) {
        // mark 根据规定的采样数量滑动采样指针
        this.current = (this.current + 1) % config.samples();
        // mark 如果采样指针大于采样数量，说明还没有达到规定的的采样数量 创建Sample
        if (this.current >= samples.size()) {
            Sample sample = newSample(timeMs);
            this.samples.add(sample);
            return sample;
            // mark 否则重设Sample
        } else {
            Sample sample = current(timeMs);
            sample.reset(timeMs);
            return sample;
        }
    }

    protected Sample newSample(long timeMs) {
        return new Sample(this.initialValue, timeMs);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        purgeObsoleteSamples(config, now);
        return combine(this.samples, config, now);
    }

    /**
     * 获取当前的采样对象。
     *
     * @param timeMs 当前时间的 POSIX 时间戳（毫秒）
     * @return 当前的采样对象
     */
    public Sample current(long timeMs) {
        //noinspection SizeReplaceableByIsEmpty
        if (samples.size() == 0)
            this.samples.add(newSample(timeMs));
        return this.samples.get(this.current);
    }

    public Sample oldest(long now) {
        //noinspection SizeReplaceableByIsEmpty
        if (samples.size() == 0)
            this.samples.add(newSample(now));
        Sample oldest = this.samples.get(0);
        for (int i = 1; i < this.samples.size(); i++) {
            Sample curr = this.samples.get(i);
            if (curr.lastWindowMs < oldest.lastWindowMs)
                oldest = curr;
        }
        return oldest;
    }

    @Override
    public String toString() {
        return "SampledStat(" +
            "initialValue=" + initialValue +
            ", current=" + current +
            ", samples=" + samples +
            ')';
    }


    /**
     * 更新采样值。
     *
     * @param sample 要更新的采样对象
     * @param config 指标的配置
     * @param value 要记录的值
     * @param timeMs 记录值的 POSIX 时间戳（毫秒）
     */
    protected abstract void update(Sample sample, MetricConfig config, double value, long timeMs);

    /**
     * 组合多个采样值。
     *
     * @param samples 要组合的采样对象列表
     * @param config 指标的配置
     * @param now 当前时间的 POSIX 时间戳（毫秒）
     * @return 组合后的结果值
     */
    public abstract double combine(List<Sample> samples, MetricConfig config, long now);

    /* Timeout any windows that have expired in the absence of any events */
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
        long expireAge = config.samples() * config.timeWindowMs();
        for (Sample sample : samples) {
            if (now - sample.lastWindowMs >= expireAge)
                sample.reset(now);
        }
    }

    protected static class Sample {
        public double initialValue;
        public long eventCount;
        public long lastWindowMs;
        public double value;

        public Sample(double initialValue, long now) {
            // mark 初始值
            this.initialValue = initialValue;
            // mark 事件计数
            this.eventCount = 0;
            // mark 最后记录时间
            this.lastWindowMs = now;
            // mark 采样值
            this.value = initialValue;
        }

        /**
         * 重置采样对象的状态。
         *
         * @param now 当前时间的 POSIX 时间戳（毫秒）
         */
        public void reset(long now) {
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        /**
         * 检查采样是否完成
         *
         * @param timeMs 当前时间的 POSIX 时间戳（毫秒）
         * @param config 指标的配置
         * @return 如果时间窗口已过或事件计数达到指定窗口大小，则返回 true；否则返回 false
         */
        public boolean isComplete(long timeMs, MetricConfig config) {
            // mark 采样完成的条件 当前时间减去上次记录时间 大于等于窗口时间大小 或者 时间事件计数 大于等于窗口事件计数
            return timeMs - lastWindowMs >= config.timeWindowMs() || eventCount >= config.eventWindow();
        }

        @Override
        public String toString() {
            return "Sample(" +
                "value=" + value +
                ", eventCount=" + eventCount +
                ", lastWindowMs=" + lastWindowMs +
                ", initialValue=" + initialValue +
                ')';
        }
    }

}
