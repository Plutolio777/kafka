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

import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Locale;
import java.util.Objects;

/**
 * 此最终类表示监听器名称，提供了根据安全协议或给定值创建实例的方法，
 * 以及处理和返回相应配置前缀的方法。
 */
public final class ListenerName {

    private static final String CONFIG_STATIC_PREFIX = "listener.name";

    /**
     * 使用安全协议的名称创建ListenerName的实例。
     *
     * @param securityProtocol 安全协议，用于根据其名称创建ListenerName实例。
     * @return 新建的ListenerName实例。
     * 根据安全协议名称创建实例。
     */
    public static ListenerName forSecurityProtocol(SecurityProtocol securityProtocol) {
        return new ListenerName(securityProtocol.name);
    }

    /**
     * mark 创建一个实例，其中提供的值会被转换为大写。
     *
     * @param value 用于创建ListenerName实例的字符串值，将会被转换成大写形式。
     * @return 新建的ListenerName实例。
     * 根据提供的值（转换为大写）创建实例。
     */
    public static ListenerName normalised(String value) {
        return new ListenerName(value.toUpperCase(Locale.ROOT));
    }

    private final String value;

    public ListenerName(String value) {
        Objects.requireNonNull(value, "value should not be null");
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ListenerName))
            return false;
        ListenerName that = (ListenerName) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "ListenerName(" + value + ")";
    }

    /**
     * 构建配置前缀字符串。 (listener.name.{name}.)
     *
     * @return 返回构建好的配置前缀字符串。
     */
    public String configPrefix() {
        // mark 拼接配置前缀，静态前缀、动态值（转换为小写）和连接符号。
        return CONFIG_STATIC_PREFIX + "." + value.toLowerCase(Locale.ROOT) + ".";
    }


    public String saslMechanismConfigPrefix(String saslMechanism) {
        return configPrefix() + saslMechanismPrefix(saslMechanism);
    }

    public static String saslMechanismPrefix(String saslMechanism) {
        return saslMechanism.toLowerCase(Locale.ROOT) + ".";
    }
}
