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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Thread that sets things up nicely
 */
/**
 * 自定义的Kafka线程类，扩展自Java的Thread类，提供了一些方便创建和配置线程的构造函数。
 */
public class KafkaThread extends Thread {

    private final Logger log = LoggerFactory.getLogger(getClass()); // 日志记录器

    /**
     * 创建一个守护（daemon）线程。
     *
     * @param name 线程名称。
     * @param runnable 线程执行的Runnable对象。
     * @return 返回配置为守护线程的KafkaThread实例。
     */
    public static KafkaThread daemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, true);
    }

    /**
     * 创建一个非守护（non-daemon）线程。
     *
     * @param name 线程名称。
     * @param runnable 线程执行的Runnable对象。
     * @return 返回配置为非守护线程的KafkaThread实例。
     */
    public static KafkaThread nonDaemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, false);
    }

    /**
     * 构造函数，用于创建一个指定名称和守护状态的线程。
     *
     * @param name 线程名称。
     * @param daemon 线程是否为守护线程。
     */
    public KafkaThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    /**
     * 构造函数，用于创建一个指定名称、执行任务和守护状态的线程。
     *
     * @param name 线程名称。
     * @param runnable 线程执行的Runnable对象。
     * @param daemon 线程是否为守护线程。
     */
    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }

    /**
     * 配置线程的守护状态和未捕获异常处理器。
     *
     * @param name 线程名称。
     * @param daemon 线程是否为守护线程。
     */
    private void configureThread(final String name, boolean daemon) {
        setDaemon(daemon); // 设置线程的守护状态
        // 设置线程的未捕获异常处理器，统一处理线程中未捕获的异常
        setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", name, e));
    }

}
