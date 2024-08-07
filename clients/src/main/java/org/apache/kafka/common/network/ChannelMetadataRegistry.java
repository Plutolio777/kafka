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
package org.apache.kafka.common.network;

import java.io.Closeable;

/**
 * 有关通道的元数据在网络堆栈的各个位置提供。这
 * 注册表被用作收集它们的公共场所。
 */
public interface ChannelMetadataRegistry extends Closeable {

    /**
     * 注册有关我们正在使用的 SSL 密码的信息。
     * 重新注册信息将覆盖之前的信息。
     */
    void registerCipherInformation(CipherInformation cipherInformation);

    /**
     * 获取当前注册的密码信息。
     */
    CipherInformation cipherInformation();

    /**
     * 注册我们正在使用的客户端信息。
     * 根据客户端的不同，可以接收 ApiVersionsRequest
     * 多次或根本不。重新登记信息即可
     * 覆盖前一个。
     */
    void registerClientInformation(ClientInformation clientInformation);

    /**
     * Get the currently registered client information.
     */
    ClientInformation clientInformation();

    /**
     * Unregister everything that has been registered and close the registry.
     */
    void close();
}
