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

package org.apache.kafka.common.utils;

import java.util.concurrent.ThreadLocalRandom;

/**
 * mark 指数退避算法的实现
 * 一个用于保持参数并提供指数回退、指数重新连接回退、指数超时等值的工具类。
 * 公式为：
 * Backoff(attempts) = random(1 - jitter, 1 + jitter) * initialInterval * multiplier ^ attempts
 *                          抖动值                           初始时间间隔       乘数
 * 如果 initialInterval 大于或等于 maxInterval，则提供一个固定的回退值。
 * 此类是线程安全的。
 */
public class ExponentialBackoff {
    private final int multiplier;
    private final double expMax;
    private final long initialInterval;
    private final double jitter;

    public ExponentialBackoff(long initialInterval, int multiplier, long maxInterval, double jitter) {
        this.initialInterval = initialInterval;
        this.multiplier = multiplier;
        this.jitter = jitter;
        // mark 计算一个最大指数位
        this.expMax = maxInterval > initialInterval ?
                Math.log(maxInterval / (double) Math.max(initialInterval, 1)) / Math.log(multiplier) : 0;
    }

    public long backoff(long attempts) {
        if (expMax == 0) {
            return initialInterval;
        }
        double exp = Math.min(attempts, this.expMax);
        double term = initialInterval * Math.pow(multiplier, exp);
        double randomFactor = jitter < Double.MIN_NORMAL ? 1.0 :
            ThreadLocalRandom.current().nextDouble(1 - jitter, 1 + jitter);
        return (long) (randomFactor * term);
    }
}
