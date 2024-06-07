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

package org.apache.kafka.metadata;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个 BrokerState 枚举类定义了 Kafka broker 在其生命周期中的各种状态。每个状态代表 broker 在不同操作阶段的具体情况。以下是各个状态的详细说明：
 * The expected state transitions are:
 * NOT_RUNNING
 *     ↓
 * STARTING
 *     ↓
 * RECOVERY
 *     ↓
 * RUNNING
 *     ↓
 * PENDING_CONTROLLED_SHUTDOWN
 *     ↓
 * SHUTTING_DOWN
 */
@InterfaceStability.Evolving
public enum BrokerState {
    /**
     * broker 初次启动时所处的状态。
     */
    NOT_RUNNING((byte) 0),

    /**
     * broker 启动并正在与集群元数据同步时的状态。
     */
    STARTING((byte) 1),

    /**
     * broker 已经与集群元数据同步，但尚未被控制器解除隔离。
     */
    RECOVERY((byte) 2),

    /**
     * broker 至少已注册过一次并正在接受客户端请求时的状态。
     */
    RUNNING((byte) 3),

    /**
     * broker 尝试执行受控关闭时的状态。
     */
    PENDING_CONTROLLED_SHUTDOWN((byte) 6),

    /**
     * broker 正在关闭时的状态。
     */
    SHUTTING_DOWN((byte) 7),

    /**
     * broker 处于未知状态。
     */
    UNKNOWN((byte) 127);

    private final static Map<Byte, BrokerState> VALUES_TO_ENUMS = new HashMap<>();

    static {
        for (BrokerState state : BrokerState.values()) {
            VALUES_TO_ENUMS.put(state.value(), state);
        }
    }

    private final byte value;

    BrokerState(byte value) {
        this.value = value;
    }

    /**
     * 根据字节值获取BrokerState枚举实例。
     * 此方法通过字节值来反向映射到BrokerState枚举常量。它首先尝试从预定义的映射中获取对应的枚举常量。
     * 如果字节值对应的枚举常量不存在，则返回UNKNOWN状态，表示该字节值代表的状态未知。
     *
     * @param value 字节值，代表某个Broker状态。
     * @return 对应的BrokerState枚举实例，如果不存在则返回UNKNOWN。
     */
    public static BrokerState fromValue(byte value) {
        BrokerState state = VALUES_TO_ENUMS.get(value);
        if (state == null) {
            return UNKNOWN;
        }
        return state;
    }

    public byte value() {
        return value;
    }
}
