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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * 代表一个抽象的请求/响应接口。
 * <p>
 * 这个接口定义了一个通用的请求或响应类型的基类。它提供了一个方法来获取与请求或响应相关的数据。
 * </p>
 */
public interface AbstractRequestResponse {

    /**
     * 获取与请求或响应相关的数据。(卡夫卡将所有的请求响应数据都定义在json中生成protobuf Message)
     *
     * @return 一个 {@link ApiMessage} 对象，包含请求或响应的数据。
     */
    ApiMessage data();
}
