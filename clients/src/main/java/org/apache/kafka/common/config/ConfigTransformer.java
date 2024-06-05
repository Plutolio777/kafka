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
import org.apache.kafka.common.config.provider.FileConfigProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class wraps a set of {@link ConfigProvider} instances and uses them to perform
 * transformations.
 *
 * <p>The default variable pattern is of the form <code>${provider:[path:]key}</code>,
 * where the <code>provider</code> corresponds to a {@link ConfigProvider} instance, as passed to
 * {@link ConfigTransformer#ConfigTransformer(Map)}.  The pattern will extract a set
 * of paths (which are optional) and keys and then pass them to {@link ConfigProvider#get(String, Set)} to obtain the
 * values with which to replace the variables.
 *
 * <p>For example, if a Map consisting of an entry with a provider name "file" and provider instance
 * {@link FileConfigProvider} is passed to the {@link ConfigTransformer#ConfigTransformer(Map)}, and a Properties
 * file with contents
 * <pre>
 * fileKey=someValue
 * </pre>
 * resides at the path "/tmp/properties.txt", then when a configuration Map which has an entry with a key "someKey" and
 * a value "${file:/tmp/properties.txt:fileKey}" is passed to the {@link #transform(Map)} method, then the transformed
 * Map will have an entry with key "someKey" and a value "someValue".
 *
 * <p>This class only depends on {@link ConfigProvider#get(String, Set)} and does not depend on subscription support
 * in a {@link ConfigProvider}, such as the {@link ConfigProvider#subscribe(String, Set, ConfigChangeCallback)} and
 * {@link ConfigProvider#unsubscribe(String, Set, ConfigChangeCallback)} methods.
 */
public class ConfigTransformer {
    public static final Pattern DEFAULT_PATTERN = Pattern.compile("\\$\\{([^}]*?):(([^}]*?):)?([^}]*?)\\}");
    private static final String EMPTY_PATH = "";

    private final Map<String, ConfigProvider> configProviders;

    /**
     * Creates a ConfigTransformer with the default pattern, of the form <code>${provider:[path:]key}</code>.
     *
     * @param configProviders a Map of provider names and {@link ConfigProvider} instances.
     */
    public ConfigTransformer(Map<String, ConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * 使用{@link ConfigProvider}实例查找值以替换模式中的变量，来转换给定的配置数据。
     *
     * @param configs 要被转换的配置值映射
     * @return {@link ConfigTransformerResult}实例
     */
    public ConfigTransformerResult transform(Map<String, String> configs) {
        Map<String, Map<String, Set<String>>> keysByProvider = new HashMap<>(); // 用于存储按提供者分类的键集合
        Map<String, Map<String, Map<String, String>>> lookupsByProvider = new HashMap<>(); // 按提供者分类的查找值映射

        // mark 从给定的配置中收集需要转换的变量  keysByProvider -> Map{provider_name -> Map{path -> Set(val1, val2 , val3)}}
        for (Map.Entry<String, String> config : configs.entrySet()) {
            if (config.getValue() != null) {
                // mark 使用正则匹配配置中的中间配置 ${provider_name(:value):key}
                List<ConfigVariable> vars = getVars(config.getValue(), DEFAULT_PATTERN);
                // mark 遍历所有的中间配置 解析的中间配置  ${provider_name(:path):key} 会转化为 ConfigVariable(provider_name, path, value)
                for (ConfigVariable var : vars) {
                    Map<String, Set<String>> keysByPath = keysByProvider.computeIfAbsent(var.providerName, k -> new HashMap<>());
                    Set<String> keys = keysByPath.computeIfAbsent(var.path, k -> new HashSet<>());
                    keys.add(var.variable); // 将变量添加到对应的提供者和路径下
                }
            }
        }


        // Retrieve requested variables from the ConfigProviders
        Map<String, Long> ttls = new HashMap<>();
        // mark 遍历keysByProvider集合
        for (Map.Entry<String, Map<String, Set<String>>> entry : keysByProvider.entrySet()) {
            // mark 根据key获取provider的名称和实例
            String providerName = entry.getKey();
            ConfigProvider provider = configProviders.get(providerName);
            // mark keysByPath的value是 path -> Set(key1, key2 , key3)的映射

            Map<String, Set<String>> keysByPath = entry.getValue();
            if (provider != null && keysByPath != null) {
                // mark 开始遍历vals
                for (Map.Entry<String, Set<String>> pathWithKeys : keysByPath.entrySet()) {
                    String path = pathWithKeys.getKey();
                    Set<String> keys = new HashSet<>(pathWithKeys.getValue());
                    // mark provider的get方法会根据path和value集合获取对应的ConfigData 所以如果自定义需要实现ConfigProvider.get方法
                    // mark ConfigData是对Set(key1, key2 , key3) 其中的data属性为获取到的键值对 一下是一个实例
                    /*
                     *   @Override
                     *   public ConfigData get(String path, Set<String> keys) {
                     *       // 根据指定的键获取配置数据
                     *       Map<String, String> data = new HashMap<>();
                     *       for (String key : keys) {
                     *           String value = System.getenv(key);
                     *           if (value != null) {
                     *               data.put(key, value);
                     *           }
                     *       }
                     *       return new ConfigData(data);
                     *   }
                     */
                    ConfigData configData = provider.get(path, keys);
                    Map<String, String> data = configData.data();
                    Long ttl = configData.ttl();
                    if (ttl != null && ttl >= 0) {
                        ttls.put(path, ttl);
                    }
                    // mark 将键值对保存到 lookupsByProvider 中 lookupsByProvider -> Map{providerName->Map{path-<Map{key->value}}}
                    Map<String, Map<String, String>> keyValuesByPath =
                            lookupsByProvider.computeIfAbsent(providerName, k -> new HashMap<>());
                    keyValuesByPath.put(path, data);
                }
            }
        }

        // Perform the transformations by performing variable replacements
        Map<String, String> data = new HashMap<>(configs);
        for (Map.Entry<String, String> config : configs.entrySet()) {
            data.put(config.getKey(), replace(lookupsByProvider, config.getValue(), DEFAULT_PATTERN));
        }
        // mark 返回结果 data为经过ConfigProvider处理过后的完整配置
        return new ConfigTransformerResult(data, ttls);
    }

    private static List<ConfigVariable> getVars(String value, Pattern pattern) {
        List<ConfigVariable> configVars = new ArrayList<>();
        Matcher matcher = pattern.matcher(value);
        while (matcher.find()) {
            configVars.add(new ConfigVariable(matcher));
        }
        return configVars;
    }

    /**
     * 使用给定的查找表替换字符串中匹配的变量。
     *
     * @param lookupsByProvider Map{providerName->Map{path-<Map{key->value}}} ConfigProvider获取的映射
     * @param value             配置文件中的原始value
     * @param pattern           用于在原始字符串中识别变量的正则表达式模式。
     * @return 替换变量后的字符串。如果输入的字符串为null，则返回null。
     */
    private static String replace(Map<String, Map<String, Map<String, String>>> lookupsByProvider,
                                  String value,
                                  Pattern pattern) {
        if (value == null) {
            return null;
        }
        // mark 这里又用正则把value匹配一下
        Matcher matcher = pattern.matcher(value);
        // mark 这里又StringBuilder的原因是可能是有组合式的值比如 ${provider_name(:path):key},val1,val2
        StringBuilder builder = new StringBuilder();
        int i = 0;
        // 遍历字符串中所有匹配的变量
        while (matcher.find()) {

            ConfigVariable configVar = new ConfigVariable(matcher);
            // mark 根据providerName获取 path到 键值对的映射
            Map<String, Map<String, String>> lookupsByPath = lookupsByProvider.get(configVar.providerName);
            if (lookupsByPath != null) {
                // mark 根据path获取key value 键值对映射
                Map<String, String> keyValues = lookupsByPath.get(configVar.path);
                // mark replacement 为真正的配置值
                String replacement = keyValues.get(configVar.variable);
                // mark 把中间配置之前的值保存在builder中
                builder.append(value, i, matcher.start());
                if (replacement == null) {
                    // mark 如果没有找到替换值，就保留原始的变量字符串
                    builder.append(matcher.group(0));
                } else {
                    // mark 使用找到的替换值替换原始变量
                    builder.append(replacement);
                }
                i = matcher.end();
            }
        }
        // mark 将字符串中最后剩余的部分添加到构建器中
        builder.append(value, i, value.length());
        return builder.toString();
    }

    /**
     * mark 对kafka配置中${provider_name(:path):key}占位符的封装 用于提供给Config Provider获取真正的配置值
     * 这是一个私有静态内部类，用于解析和存储配置项的提供商名称、路径和变量名。
     */
    private static class ConfigVariable {
        final String providerName; // 提供者名称
        final String path; // 路径，如果不存在则为空
        final String variable; // 变量名

        /**
         * 构造函数，用于初始化配置变量。
         * @param matcher 一个匹配器对象，用于从配置项字符串中提取提供商名称、路径和变量名。
         */
        ConfigVariable(Matcher matcher) {
            this.providerName = matcher.group(1); // 提取提供商名称
            this.path = matcher.group(3) != null ? matcher.group(3) : EMPTY_PATH; // 提取路径，如果不存在则使用空路径
            this.variable = matcher.group(4); // 提取变量名
        }

        /**
         * 重写toString方法，以便于以字符串形式输出配置变量。
         * @return 返回配置变量的字符串表示，格式为"(提供商名称:路径:变量名)"。
         */
        public String toString() {
            return "(" + providerName + ":" + (path != null ? path + ":" : "") + variable + ")";
        }
    }
}
