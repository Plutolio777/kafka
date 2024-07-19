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
package org.apache.kafka.common.feature;

import static java.util.stream.Collectors.joining;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.utils.Utils;

/**
 * 表示一个不可变的基本版本范围，使用两个属性：min 和 max，类型均为 short。
 * min 和 max 属性需要满足以下两个规则：
 *  - 它们都应大于或等于 1，因为我们只认为正的版本值是有效的。
 *  - max 应大于或等于 min。
 *  该类还提供了一个将版本范围转换为映射（map）的 API。该类允许配置 min/max 属性的标签，这些标签可以由子类进行专门化（如果需要的话）。
 */
class BaseVersionRange {
    // Non-empty label for the min version key, that's used only to convert to/from a map.
    private final String minKeyLabel;

    // The value of the minimum version.
    private final short minValue;

    // Non-empty label for the max version key, that's used only to convert to/from a map.
    private final String maxKeyLabel;

    // The value of the maximum version.
    private final short maxValue;

    /**
     * 除非满足以下条件，否则抛出异常：
     * minValue >= 1 并且 maxValue >= 1 并且 maxValue >= minValue。
     *
     * @param minKeyLabel   最小版本键的标签，仅用于从映射（map）中转换。
     * @param minValue      最小版本值。
     * @param maxKeyLabel   最大版本键的标签，仅用于从映射（map）中转换。
     * @param maxValue      最大版本值。
     *
     * @throws IllegalArgumentException   如果以下任一条件为真，则抛出异常：
     *                                     - (minValue < 1) 或 (maxValue < 1) 或 (maxValue < minValue)。
     *                                     - minKeyLabel 为空，或 minKeyLabel 为空。
     */
    protected BaseVersionRange(String minKeyLabel, short minValue, String maxKeyLabel, short maxValue) {
        // mark 不满足条件抛出异常
        if (minValue < 1 || maxValue < 1 || maxValue < minValue) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minValue >= 1, maxValue >= 1 and maxValue >= minValue, but received" +
                    " minValue: %d, maxValue: %d", minValue, maxValue));
        }
        if (minKeyLabel.isEmpty()) {
            throw new IllegalArgumentException("Expected minKeyLabel to be non-empty.");
        }
        if (maxKeyLabel.isEmpty()) {
            throw new IllegalArgumentException("Expected maxKeyLabel to be non-empty.");
        }
        this.minKeyLabel = minKeyLabel;
        this.minValue = minValue;
        this.maxKeyLabel = maxKeyLabel;
        this.maxValue = maxValue;
    }

    public short min() {
        return minValue;
    }

    public short max() {
        return maxValue;
    }

    public String toString() {
        return String.format(
            "%s[%s]",
            this.getClass().getSimpleName(),
            mapToString(toMap()));
    }

    public Map<String, Short> toMap() {
        return Utils.mkMap(Utils.mkEntry(minKeyLabel, min()), Utils.mkEntry(maxKeyLabel, max()));
    }

    private static String mapToString(final Map<String, Short> map) {
        return map
            .entrySet()
            .stream()
            .map(entry -> String.format("%s:%d", entry.getKey(), entry.getValue()))
            .collect(joining(", "));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final BaseVersionRange that = (BaseVersionRange) other;
        return Objects.equals(this.minKeyLabel, that.minKeyLabel) &&
            this.minValue == that.minValue &&
            Objects.equals(this.maxKeyLabel, that.maxKeyLabel) &&
            this.maxValue == that.maxValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minKeyLabel, minValue, maxKeyLabel, maxValue);
    }

    public static short valueOrThrow(String key, Map<String, Short> versionRangeMap) {
        final Short value = versionRangeMap.get(key);
        if (value == null) {
            throw new IllegalArgumentException(String.format("%s absent in [%s]", key, mapToString(versionRangeMap)));
        }
        return value;
    }
}
