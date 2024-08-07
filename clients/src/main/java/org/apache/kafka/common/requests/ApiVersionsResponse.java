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

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKeyCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKeyCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordVersion;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Possible error codes:
 * - {@link Errors#UNSUPPORTED_VERSION}
 * - {@link Errors#INVALID_REQUEST}
 */
public class ApiVersionsResponse extends AbstractResponse {

    public static final long UNKNOWN_FINALIZED_FEATURES_EPOCH = -1L;

    private final ApiVersionsResponseData data;

    public ApiVersionsResponse(ApiVersionsResponseData data) {
        super(ApiKeys.API_VERSIONS);
        this.data = data;
    }

    @Override
    public ApiVersionsResponseData data() {
        return data;
    }

    public ApiVersion apiVersion(short apiKey) {
        return data.apiKeys().find(apiKey);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(this.data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    public boolean zkMigrationReady() {
        return data.zkMigrationReady();
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer, short version) {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it which means that the version received by this
        // method is not necessarily the real one. It may be version 0 as well.
        int prev = buffer.position();
        try {
            return new ApiVersionsResponse(new ApiVersionsResponseData(new ByteBufferAccessor(buffer), version));
        } catch (RuntimeException e) {
            buffer.position(prev);
            if (version != 0)
                return new ApiVersionsResponse(new ApiVersionsResponseData(new ByteBufferAccessor(buffer), (short) 0));
            else
                throw e;
        }
    }

    public static ApiVersionsResponse defaultApiVersionsResponse(
        ApiMessageType.ListenerType listenerType
    ) {
        return defaultApiVersionsResponse(0, listenerType);
    }

    public static ApiVersionsResponse defaultApiVersionsResponse(
        int throttleTimeMs,
        ApiMessageType.ListenerType listenerType
    ) {
        return createApiVersionsResponse(throttleTimeMs, filterApis(RecordVersion.current(), listenerType), Features.emptySupportedFeatures());
    }

    public static ApiVersionsResponse createApiVersionsResponse(
        int throttleTimeMs,
        ApiVersionCollection apiVersions
    ) {
        return createApiVersionsResponse(throttleTimeMs, apiVersions, Features.emptySupportedFeatures());
    }

    public static ApiVersionsResponse createApiVersionsResponse(
        int throttleTimeMs,
        ApiVersionCollection apiVersions,
        Features<SupportedVersionRange> latestSupportedFeatures
    ) {
        return createApiVersionsResponse(
            throttleTimeMs,
            apiVersions,
            latestSupportedFeatures,
            Collections.emptyMap(),
            UNKNOWN_FINALIZED_FEATURES_EPOCH);
    }

    public static ApiVersionsResponse createApiVersionsResponse(
        int throttleTimeMs,
        RecordVersion minRecordVersion,
        Features<SupportedVersionRange> latestSupportedFeatures,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch,
        NodeApiVersions controllerApiVersions,
        ListenerType listenerType
    ) {
        ApiVersionCollection apiKeys;
        if (controllerApiVersions != null) {
            apiKeys = intersectForwardableApis(
                listenerType, minRecordVersion, controllerApiVersions.allSupportedApiVersions());
        } else {
            apiKeys = filterApis(minRecordVersion, listenerType);
        }

        return createApiVersionsResponse(
            throttleTimeMs,
            apiKeys,
            latestSupportedFeatures,
            finalizedFeatures,
            finalizedFeaturesEpoch
        );
    }

    public static ApiVersionsResponse createApiVersionsResponse(
        int throttleTimeMs,
        ApiVersionCollection apiVersions,
        Features<SupportedVersionRange> latestSupportedFeatures,
        Map<String, Short> finalizedFeatures,
        long finalizedFeaturesEpoch
    ) {
        return new ApiVersionsResponse(
            createApiVersionsResponseData(
                throttleTimeMs,
                Errors.NONE,
                apiVersions,
                latestSupportedFeatures,
                finalizedFeatures,
                finalizedFeaturesEpoch
            )
        );
    }

    public static ApiVersionCollection filterApis(
        RecordVersion minRecordVersion,
        ApiMessageType.ListenerType listenerType
    ) {
        ApiVersionCollection apiKeys = new ApiVersionCollection();
        for (ApiKeys apiKey : ApiKeys.apisForListener(listenerType)) {
            if (apiKey.minRequiredInterBrokerMagic <= minRecordVersion.value) {
                apiKeys.add(ApiVersionsResponse.toApiVersion(apiKey));
            }
        }
        return apiKeys;
    }

    public static ApiVersionCollection collectApis(Set<ApiKeys> apiKeys) {
        ApiVersionCollection res = new ApiVersionCollection();
        for (ApiKeys apiKey : apiKeys) {
            res.add(ApiVersionsResponse.toApiVersion(apiKey));
        }
        return res;
    }

    /**
     * Find the common range of supported API versions between the locally
     * known range and that of another set.
     *
     * @param listenerType the listener type which constrains the set of exposed APIs
     * @param minRecordVersion min inter broker magic
     * @param activeControllerApiVersions controller ApiVersions
     * @return commonly agreed ApiVersion collection
     */
    public static ApiVersionCollection intersectForwardableApis(
        final ApiMessageType.ListenerType listenerType,
        final RecordVersion minRecordVersion,
        final Map<ApiKeys, ApiVersion> activeControllerApiVersions
    ) {
        ApiVersionCollection apiKeys = new ApiVersionCollection();
        for (ApiKeys apiKey : ApiKeys.apisForListener(listenerType)) {
            if (apiKey.minRequiredInterBrokerMagic <= minRecordVersion.value) {
                ApiVersion brokerApiVersion = toApiVersion(apiKey);

                final ApiVersion finalApiVersion;
                if (!apiKey.forwardable) {
                    finalApiVersion = brokerApiVersion;
                } else {
                    Optional<ApiVersion> intersectVersion = intersect(brokerApiVersion,
                        activeControllerApiVersions.getOrDefault(apiKey, null));
                    if (intersectVersion.isPresent()) {
                        finalApiVersion = intersectVersion.get();
                    } else {
                        // Controller doesn't support this API key, or there is no intersection.
                        continue;
                    }
                }

                apiKeys.add(finalApiVersion.duplicate());
            }
        }
        return apiKeys;
    }

    private static ApiVersionsResponseData createApiVersionsResponseData(
        final int throttleTimeMs,
        final Errors error,
        final ApiVersionCollection apiKeys,
        final Features<SupportedVersionRange> latestSupportedFeatures,
        final Map<String, Short> finalizedFeatures,
        final long finalizedFeaturesEpoch
    ) {
        final ApiVersionsResponseData data = new ApiVersionsResponseData();
        data.setThrottleTimeMs(throttleTimeMs);
        data.setErrorCode(error.code());
        data.setApiKeys(apiKeys);
        data.setSupportedFeatures(createSupportedFeatureKeys(latestSupportedFeatures));
        data.setFinalizedFeatures(createFinalizedFeatureKeys(finalizedFeatures));
        data.setFinalizedFeaturesEpoch(finalizedFeaturesEpoch);

        return data;
    }

    private static SupportedFeatureKeyCollection createSupportedFeatureKeys(
        Features<SupportedVersionRange> latestSupportedFeatures) {
        SupportedFeatureKeyCollection converted = new SupportedFeatureKeyCollection();
        for (Map.Entry<String, SupportedVersionRange> feature : latestSupportedFeatures.features().entrySet()) {
            final SupportedFeatureKey key = new SupportedFeatureKey();
            final SupportedVersionRange versionRange = feature.getValue();
            key.setName(feature.getKey());
            key.setMinVersion(versionRange.min());
            key.setMaxVersion(versionRange.max());
            converted.add(key);
        }

        return converted;
    }

    private static FinalizedFeatureKeyCollection createFinalizedFeatureKeys(
        Map<String, Short> finalizedFeatures) {
        FinalizedFeatureKeyCollection converted = new FinalizedFeatureKeyCollection();
        for (Map.Entry<String, Short> feature : finalizedFeatures.entrySet()) {
            final FinalizedFeatureKey key = new FinalizedFeatureKey();
            final short versionLevel = feature.getValue();
            key.setName(feature.getKey());
            key.setMinVersionLevel(versionLevel);
            key.setMaxVersionLevel(versionLevel);
            converted.add(key);
        }

        return converted;
    }

    /**
     * 计算两个ApiVersion对象的交集。
     * 交集的定义是具有相同apiKey且版本号范围在两个版本范围内的ApiVersion。
     *
     * @param thisVersion 当前版本对象，不可为null。
     * @param other       另一个版本对象，不可为null。
     * @return 交集版本对象的Optional。如果apiKey不匹配或交集不存在，则返回空。
     * @throws IllegalArgumentException 如果apiKey不匹配，则抛出此异常。
     */
    public static Optional<ApiVersion> intersect(ApiVersion thisVersion,
                                                 ApiVersion other) {
        // mark 检查参数是否为null
        if (thisVersion == null || other == null) return Optional.empty();

        // mark 检查apiKey是否相同，如果不相同则抛出异常
        if (thisVersion.apiKey() != other.apiKey())
            throw new IllegalArgumentException("thisVersion.apiKey: " + thisVersion.apiKey()
                + " must be equal to other.apiKey: " + other.apiKey());

        // mark 计算最大最小版本号，保证minVersion不大于maxVersion
        short minVersion = (short) Math.max(thisVersion.minVersion(), other.minVersion());
        short maxVersion = (short) Math.min(thisVersion.maxVersion(), other.maxVersion());

        // mark 如果最小版本大于最大版本，说明没有交集，返回空的Optional
        if (minVersion > maxVersion)
            return Optional.empty();
        else {
            // mark 创建并返回新的ApiVersion对象，表示交集
            return Optional.of(new ApiVersion()
                    .setApiKey(thisVersion.apiKey())
                    .setMinVersion(minVersion)
                    .setMaxVersion(maxVersion));
        }
    }


    public static ApiVersion toApiVersion(ApiKeys apiKey) {
        return new ApiVersion()
            .setApiKey(apiKey.id)
            .setMinVersion(apiKey.oldestVersion())
            .setMaxVersion(apiKey.latestVersion());
    }
}
