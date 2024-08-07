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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * 通道认证顶级接口
 */
public interface Authenticator extends Closeable {
    /**
     * 在使用transportLayer对socket channel进行读写是需要进行身份认证
     * 对于安全协议 PLAINTEXT 和 SSL，这是一个无操作，因为不需要进一步的身份验证。
     * 对于 SASL_PLAINTEXT 和 SASL_SSL，这会执行 SASL 身份验证。
     *
     * @throws AuthenticationException 如果由于凭据无效或其他安全配置错误而导致身份验证失败
     * @throws IOException 如果由于 I/O 错误导致读取/写入失败
     */
    void authenticate() throws AuthenticationException, IOException;

    /**
     * 处理与身份验证失败相关的任何操作。当通道因为先前的 {@link #authenticate()} 调用抛出的
     * {@link AuthenticationException} 而即将关闭时，将调用此方法。
     * @throws IOException 如果由于 I/O 错误导致读取/写入失败
     */
    default void handleAuthenticationFailure() throws IOException {
    }

    /**
     * 使用PrincipalBuilder返回Principal
     */
    KafkaPrincipal principal();

    /**
     * 返回主体的序列化器/反序列化器接口
     */
    Optional<KafkaPrincipalSerde> principalSerde();

    /**
     * 如果验证完成则返回 true，否则返回 false；
     */
    boolean complete();

    /**
     * 开始重新进行身份验证。使用 transportLayer 读取或写入令牌，方法与 {@link #authenticate()} 相同。
     * 对于安全协议 PLAINTEXT 和 SSL，这是一项无操作，因为重新身份验证不适用/不被支持。
     * 对于 SASL_PLAINTEXT 和 SASL_SSL，这将执行 SASL 身份验证。任何来自先前请求的待处理响应
     * 可以/将被读取并收集以备后续处理。请求不得部分写入；任何排队等待写入的请求（尚未写入字节）
     * 将保持排队状态，直到重新身份验证成功之后。
     *
     * @param reauthenticationContext
     *            重新身份验证发生的上下文。此实例负责关闭之前由
     *            {@link ReauthenticationContext#previousAuthenticator()} 返回的 Authenticator。
     * @throws AuthenticationException
     *             如果由于凭据无效或其他安全配置错误导致身份验证失败
     * @throws IOException
     *             如果由于 I/O 错误导致读取/写入失败
     */
    default void reauthenticate(ReauthenticationContext reauthenticationContext) throws IOException {
        // empty
    }

    /**
     * Return the session expiration time, if any, otherwise null. The value is in
     * nanoseconds as per {@code System.nanoTime()} and is therefore only useful
     * when compared to such a value -- it's absolute value is meaningless. This
     * value may be non-null only on the server-side. It represents the time after
     * which, in the absence of re-authentication, the broker will close the session
     * if it receives a request unrelated to authentication. We store nanoseconds
     * here to avoid having to invoke the more expensive {@code milliseconds()} call
     * on the broker for every request
     * 
     * @return the session expiration time, if any, otherwise null
     */
    default Long serverSessionExpirationTimeNanos() {
        return null;
    }

    /**
     * 返回客户端应重新认证此会话的时间（如果有），否则返回 null。
     * 该值以纳秒为单位，如 {@code System.nanoTime()}，因此只有在与该值进行比较时才有意义 —— 它的绝对值是没有意义的。
     * 此值可能仅在客户端非空。它将在整个会话生命周期的 85% 到 95% 之间的随机时间，
     * 以考虑客户端和服务器之间的延迟，并避免由于许多会话同时重新认证而可能导致的重新认证风暴。
     *
     * @return 客户端应重新认证此会话的时间（如果有），否则为 null
     */
    default Long clientSessionReauthenticationTimeNanos() {
        return null;
    }

    /**
     * Return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable, otherwise null.
     * The server-side perspective will yield a lower value than the client-side
     * perspective of the same re-authentication because the client-side observes an
     * additional network round-trip.
     * 
     * @return the number of milliseconds that elapsed while re-authenticating this
     *         session from the perspective of this instance, if applicable,
     *         otherwise null
     */
    default Long reauthenticationLatencyMs() {
        return null;
    }

    /**
     * 返回在重新认证期间到达的与重新认证无关的下一个（始终非空但可能为空的）客户端
     * {@link NetworkReceive} 响应（如果有的话）。这些响应对应于在重新认证开始之前发送的请求；
     * 这些请求是在通道成功认证时发出的，并且响应是在重新认证过程中到达的。返回的响应将从认证器的队列中移除。
     * 在重新认证完成后发送的请求的响应仅在认证器响应队列为空时处理。
     *
     * @return 在重新认证期间到达的与重新认证无关的（始终非空但可能为空的）客户端 {@link NetworkReceive} 响应（如果有的话）
     */
    default Optional<NetworkReceive> pollResponseReceivedDuringReauthentication() {
        return Optional.empty();
    }
    
    /**
     * Return true if this is a server-side authenticator and the connected client
     * has indicated that it supports re-authentication, otherwise false
     * 
     * @return true if this is a server-side authenticator and the connected client
     *         has indicated that it supports re-authentication, otherwise false
     */
    default boolean connectedClientSupportsReauthentication() {
        return false;
    }
}
