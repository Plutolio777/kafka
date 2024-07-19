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

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * 在所有时间段内保持的非采样累计总数。
 * 这是 {@link WindowedSum} 的非采样版本。
 * 如果您只想在每次记录时将值增加1，请参见 {@link CumulativeCount}。
 */
public class CumulativeSum implements MeasurableStat {

    private double total;

    public CumulativeSum() {
        total = 0.0;
    }

    public CumulativeSum(double value) {
        total = value;
    }


    /**
     * 重写了record方法没有使用采样 只是简单记录了一下累加值
     *
     * @param config 指标配置（在此实现中未使用）
     * @param value  要记录的值
     * @param now    当前的时间戳（毫秒）
     */
    @Override
    public void record(MetricConfig config, double value, long now) {
        total += value;
    }

    @Override
    public double measure(MetricConfig config, long now) {
        return total;
    }

    @Override
    public String toString() {
        return "CumulativeSum(total=" + total + ")";
    }
}
