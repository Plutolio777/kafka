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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * 描述服务器是否应该要求或请求客户端身份验证。
 */
public enum SslClientAuth {
    // mark 强制客户端进行身份验证
    REQUIRED,
    // mark 客户端如果提供了证书就验证如果没提供就不认证
    REQUESTED,
    // mark 不需要客户端身份验证
    NONE;

    public static final List<SslClientAuth> VALUES =
            Collections.unmodifiableList(Arrays.asList(SslClientAuth.values()));

    public static SslClientAuth forConfig(String key) {
        if (key == null) {
            return SslClientAuth.NONE;
        }
        String upperCaseKey = key.toUpperCase(Locale.ROOT);
        for (SslClientAuth auth : VALUES) {
            if (auth.name().equals(upperCaseKey)) {
                return auth;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
