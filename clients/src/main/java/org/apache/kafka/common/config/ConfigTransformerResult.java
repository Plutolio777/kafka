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
package org.apache.kafka.common.config;

import org.apache.kafka.common.config.provider.ConfigProvider;

import java.util.Map;

/**
 * 从 {@link ConfigTransformer} 转换得到的结果。
 */
public class ConfigTransformerResult {

    private Map<String, Long> ttls; // 路径到TTL（生存时间）值的映射
    private Map<String, String> data; // 转换后的数据，键值对形式

    /**
     * 使用给定的数据和TTL值构造一个新的 ConfigTransformerResult 实例。
     *
     * @param data     转换后得到的数据，以键值对形式存储。
     * @param ttls     路径到其TTL（生存时间）值的映射，单位为毫秒。
     */
    public ConfigTransformerResult(Map<String, String> data, Map<String, Long> ttls) {
        this.data = data;
        this.ttls = ttls;
    }

    /**
     * 返回转换后得到的数据，如果找到对应的变量，会将其替换为 {@link ConfigProvider} 实例中的相应值。
     *
     * <p>修改返回的转换后数据不会影响 {@link ConfigProvider}，也不会影响用于转换的原始数据。
     *
     * @return 转换后得到的数据，以键值对形式存储。
     */
    public Map<String, String> data() {
        return data;
    }

    /**
     * 返回从 {@link ConfigProvider} 实例为给定路径集返回的TTL（生存时间）值（以毫秒为单位）。
     *
     * @return 路径到其TTL（生存时间）值的映射。
     */
    public Map<String, Long> ttls() {
        return ttls;
    }
}

