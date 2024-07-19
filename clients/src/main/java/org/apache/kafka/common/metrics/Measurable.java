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

/**
 * A measurable quantity that can be registered as a metric
 */
public interface Measurable extends MetricValueProvider<Double> {

    /**
     * 测量这个数量并以 double 类型返回结果
     * @param config 此指标的配置
     * @param now 进行测量时的 POSIX 时间（毫秒）
     * @return 测量得到的值
     */
    double measure(MetricConfig config, long now);

}
