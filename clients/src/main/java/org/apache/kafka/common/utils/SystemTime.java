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

import org.apache.kafka.common.errors.TimeoutException;

import java.util.function.Supplier;

/**
 * A time implementation that uses the system clock and sleep call. Use `Time.SYSTEM` instead of creating an instance
 * of this class.
 * kafka中实现的一个简单的定时任务机制
 */
public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        Utils.sleep(ms);
    }

    /**
     * 等待一个条件满足或直到指定的截止时间。该方法会同步锁定给定的对象，并在满足给定条件之前一直等待，
     * 或者直到达到指定的截止时间。如果条件在等待期间变得满足，或者在达到指定的截止时间之前，方法将返回。
     *
     * @param obj 被锁定并用于调用wait方法的对象。
     * @param condition 一个供应函数，用于检查条件是否满足。当条件满足时，该函数应返回true。
     * @param deadlineMs 等待的截止时间，以毫秒为单位。如果当前时间超过此值，将抛出TimeoutException。
     * @throws InterruptedException 如果在等待期间线程被中断。
     * @throws TimeoutException 如果在达到指定的截止时间之前条件未满足。
     */
    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        synchronized (obj) { // 获取对象锁，确保线程安全
            while (true) { // 持续检查条件直至满足或超时
                // lyj 如果条件满足则退出并释放锁
                if (condition.get()) // 检查条件是否满足
                    return;

                long currentTimeMs = milliseconds(); // 获取当前时间
                if (currentTimeMs >= deadlineMs) // 检查是否超过截止时间
                    throw new TimeoutException("Condition not satisfied before deadline");

                obj.wait(deadlineMs - currentTimeMs); // 基于剩余时间调用wait，进入等待状态
            }
        }
    }


}
