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
package org.apache.kafka.server.common;

import org.apache.kafka.common.utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a utility to capture a checkpoint in a file. It writes down to the file in the below format.
 * 该类表示用于在文件中捕获检查点的工具。它以下列格式写入文件。
 * ========= File beginning =========
 * version: int
 * entries-count: int
 * topic partition int
 * ========= File end ===============
 * 在检查点文件中，每个条目都表示为一行字符串。{@link EntryFormatter} 用于将条目转换为字符串及其逆转换。
 * Each entry is represented as a string on each line in the checkpoint file. {@link EntryFormatter} is used
 * to convert the entry into a string and vice versa.
 *
 * @param <T> entry type.
 */
public class CheckpointFile<T> {

    private final int version;
    private final EntryFormatter<T> formatter;
    private final Object lock = new Object();
    private final Path absolutePath;
    private final Path tempPath;

    public CheckpointFile(File file,
                          int version,
                          EntryFormatter<T> formatter) throws IOException {
        this.version = version;
        this.formatter = formatter;
        try {
            // Create the file if it does not exist.
            Files.createFile(file.toPath());
        } catch (FileAlreadyExistsException ex) {
            // Ignore if file already exists.
        }
        absolutePath = file.toPath().toAbsolutePath();
        tempPath = Paths.get(absolutePath.toString() + ".tmp");
    }

    public void write(Collection<T> entries) throws IOException {
        synchronized (lock) {
            // write to temp file and then swap with the existing file
            try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
                // Write the version
                writer.write(Integer.toString(version));
                writer.newLine();

                // Write the entries count
                writer.write(Integer.toString(entries.size()));
                writer.newLine();

                // Write each entry on a new line.
                for (T entry : entries) {
                    writer.write(formatter.toString(entry));
                    writer.newLine();
                }

                writer.flush();
                fileOutputStream.getFD().sync();
            }

            Utils.atomicMoveWithFallback(tempPath, absolutePath);
        }
    }

    public List<T> read() throws IOException {
        synchronized (lock) {
            try (BufferedReader reader = Files.newBufferedReader(absolutePath)) {
                CheckpointReadBuffer<T> checkpointBuffer = new CheckpointReadBuffer<>(absolutePath.toString(), reader, version, formatter);
                return checkpointBuffer.read();
            }
        }
    }

    private static class CheckpointReadBuffer<T> {

        private final String location;
        private final BufferedReader reader;
        private final int version;
        private final EntryFormatter<T> formatter;

        CheckpointReadBuffer(String location,
                             BufferedReader reader,
                             int version,
                             EntryFormatter<T> formatter) {
            this.location = location;
            this.reader = reader;
            this.version = version;
            this.formatter = formatter;
        }

        /**
         * 从检查点文件中读取条目列表。
         *
         * @return 条目的列表，如果文件为空或无法识别版本，则返回空列表。
         * @throws IOException 如果文件读取失败，版本不匹配，或文件格式不正确。
         */
        List<T> read() throws IOException {
            // mark 读取文件的第一行，作为版本号
            String line = reader.readLine();
            // mark 如果文件已经结束，返回空列表
            if (line == null)
                return Collections.emptyList();

            // mark 将读取的字符串转换为整数类型的版本号
            int readVersion = toInt(line);
            // mark 检查读取的版本号是否与期望的版本号匹配
            if (readVersion != version) {
                // 版本不匹配时，抛出IOException
                throw new IOException("Unrecognised version:" + readVersion + ", expected version: " + version
                                              + " in checkpoint file at: " + location);
            }

            // mark 读取文件的第二行，作为条目的预期数量
            line = reader.readLine();
            // mark 如果文件在此时已经结束，返回空列表
            if (line == null) {
                return Collections.emptyList();
            }

            // mark 将读取的字符串转换为整数类型的预期条目数量
            int expectedSize = toInt(line);
            // mark 初始化一个列表，用于存储读取的条目
            List<T> entries = new ArrayList<>(expectedSize);
            // 从文件中继续读取行，直到文件结束
            line = reader.readLine();
            while (line != null) {
                // mark 将检查点中的 topic partition offset 转换成元祖 Option((TopicPartition(topic, partition), offset))
                Optional<T> maybeEntry = formatter.fromString(line);
                // mark 如果不存在则抛出格式错误
                if (!maybeEntry.isPresent()) {
                    throw buildMalformedLineException(line);
                }
                // mark 保存 TopicPartition(topic, partition), offset)
                entries.add(maybeEntry.get());
                // mark 继续读取文件的下一行
                line = reader.readLine();
            }

            // mark 检查实际读取的条目数量是否与预期数量相符 不相符时，抛出IOException
            if (entries.size() != expectedSize) {
                throw new IOException("Expected [" + expectedSize + "] entries in checkpoint file ["
                                              + location + "], but found only [" + entries.size() + "]");
            }
            // mark TopicPartition(topic, partition), offset) 集合
            return entries;
        }

        /**
         * 将字符串转换为整数。如果转换失败，抛出IOException。
         *
         * @param line 要转换的字符串。
         * @return 转换后的整数值。
         * @throws IOException 如果字符串不能转换为整数。
         */
        private int toInt(String line) throws IOException {
            try {
                return Integer.parseInt(line);
            } catch (NumberFormatException e) {
                // 转换失败时，抛出IOException
                throw buildMalformedLineException(line);
            }
        }

        /**
         * 构建一个表示文件格式错误的IOException。
         *
         * @param line 造成错误的文件行。
         * @return 一个IOException实例，描述了文件格式错误。
         */
        private IOException buildMalformedLineException(String line) {
            return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line));
        }
    }


    /**
     * This is used to convert the given entry of type {@code T} into a string and vice versa.
     *
     * @param <T> entry type
     */
    public interface EntryFormatter<T> {

        /**
         * @param entry entry to be converted into string.
         * @return String representation of the given entry.
         */
        String toString(T entry);

        /**
         * @param value string representation of an entry.
         * @return entry converted from the given string representation if possible. {@link Optional#empty()} represents
         * that the given string representation could not be converted into an entry.
         */
        Optional<T> fromString(String value);
    }
}
