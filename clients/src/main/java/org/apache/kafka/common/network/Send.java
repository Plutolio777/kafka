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

import java.io.IOException;

/**
 * 该接口模拟正在进行的数据发送。
 */
public interface Send {

    /**
     * Is this send complete?
     */
    boolean completed();

    /**
     * 将此发送中的一些尚未写入的字节写入提供的通道。可能需要多次调用才能发送
     * 需完整书写
     * @param channel 要写入的通道
     * @return 写入的字节数
     * @throws IOException 如果写入失败
     */
    long writeTo(TransferableChannel channel) throws IOException;

    /**
     * Size of the send
     */
    long size();

}
