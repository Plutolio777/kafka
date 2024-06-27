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

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

/**
 * Provides a mechanism to unmap mapped and direct byte buffers.
 *
 * The implementation was inspired by the one in Lucene's MMapDirectory.
 */
public final class ByteBufferUnmapper {

    // null if unmap is not supported
    private static final MethodHandle UNMAP;

    // null if unmap is supported
    private static final RuntimeException UNMAP_NOT_SUPPORTED_EXCEPTION;

    static {
        Object unmap = null;
        RuntimeException exception = null;
        try {
            unmap = lookupUnmapMethodHandle();
        } catch (RuntimeException e) {
            exception = e;
        }
        if (unmap != null) {
            UNMAP = (MethodHandle) unmap;
            UNMAP_NOT_SUPPORTED_EXCEPTION = null;
        } else {
            UNMAP = null;
            UNMAP_NOT_SUPPORTED_EXCEPTION = exception;
        }
    }

    private ByteBufferUnmapper() {}

    /**
     * Unmap the provided mapped or direct byte buffer.
     *
     * This buffer cannot be referenced after this call, so it's highly recommended that any fields referencing it
     * should be set to null.
     *
     * @throws IllegalArgumentException if buffer is not mapped or direct.
     */
    public static void unmap(String resourceDescription, ByteBuffer buffer) throws IOException {
        if (!buffer.isDirect())
            throw new IllegalArgumentException("Unmapping only works with direct buffers");
        if (UNMAP == null)
            throw UNMAP_NOT_SUPPORTED_EXCEPTION;

        try {
            UNMAP.invokeExact(buffer);
        } catch (Throwable throwable) {
            throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, throwable);
        }
    }

    private static MethodHandle lookupUnmapMethodHandle() {
        final MethodHandles.Lookup lookup = lookup();
        try {
            if (Java.IS_JAVA9_COMPATIBLE)
                return unmapJava9(lookup);
            else
                return unmapJava7Or8(lookup);
        } catch (ReflectiveOperationException | RuntimeException e1) {
            throw new UnsupportedOperationException("Unmapping is not supported on this platform, because internal " +
                "Java APIs are not compatible with this Kafka version", e1);
        }
    }

    /**
     * mark 生成用于 Java 7 或 8 的内存解除映射的方法句柄。
     * <p>
     * 这个方法句柄大致相当于以下 Lambda 表达式：
     * <p>
     * (ByteBuffer buffer) -> {
     * sun.misc.Cleaner cleaner = ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
     * if (nonNull(cleaner))
     * cleaner.clean();
     * else
     * noop(cleaner); // 因为 MethodHandles#guardWithTest 总是需要 if 和 else 两个分支，所以需要 noop
     * }
     *
     * @param lookup 用于查找方法句柄的 MethodHandles.Lookup 对象
     * @return 用于解除内存映射的 MethodHandle
     * @throws ReflectiveOperationException 如果反射操作失败
     */
    private static MethodHandle unmapJava7Or8(MethodHandles.Lookup lookup) throws ReflectiveOperationException {
        // 获取 DirectByteBuffer 类
        Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
        // 获取 cleaner 方法
        Method m = directBufferClass.getMethod("cleaner");
        m.setAccessible(true);
        MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
        // 获取 cleaner 方法的返回类型
        Class<?> cleanerClass = directBufferCleanerMethod.type().returnType();
        // 获取 clean 方法
        MethodHandle cleanMethod = lookup.findVirtual(cleanerClass, "clean", methodType(void.class));
        // 创建 nonNull 测试的 MethodHandle
        MethodHandle nonNullTest = lookup.findStatic(ByteBufferUnmapper.class, "nonNull",
                methodType(boolean.class, Object.class)).asType(methodType(boolean.class, cleanerClass));
        // 创建 noop 的 MethodHandle
        MethodHandle noop = dropArguments(constant(Void.class, null).asType(methodType(void.class)), 0, cleanerClass);
        // 创建用于解除内存映射的 MethodHandle
        MethodHandle unmapper = filterReturnValue(directBufferCleanerMethod, guardWithTest(nonNullTest, cleanMethod, noop))
                .asType(methodType(void.class, ByteBuffer.class));
        return unmapper;
    }

    /**
     * 生成用于 Java 9 及以上版本的内存解除映射的方法句柄。
     *
     * @param lookup 用于查找方法句柄的 MethodHandles.Lookup 对象
     * @return 用于解除内存映射的 MethodHandle
     * @throws ReflectiveOperationException 如果反射操作失败
     */
    private static MethodHandle unmapJava9(MethodHandles.Lookup lookup) throws ReflectiveOperationException {
        // 获取 Unsafe 类
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        // 获取 invokeCleaner 方法
        MethodHandle unmapper = lookup.findVirtual(unsafeClass, "invokeCleaner",
                methodType(void.class, ByteBuffer.class));
        // 获取 theUnsafe 字段
        Field f = unsafeClass.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Object theUnsafe = f.get(null);
        // 将 invokeCleaner 方法绑定到 theUnsafe 对象上
        return unmapper.bindTo(theUnsafe);
    }

    private static boolean nonNull(Object o) {
        return o != null;
    }
}
