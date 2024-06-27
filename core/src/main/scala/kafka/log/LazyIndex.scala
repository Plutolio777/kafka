/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.File
import java.nio.file.{Files, NoSuchFileException}
import java.util.concurrent.locks.ReentrantLock

import LazyIndex._
import kafka.utils.CoreUtils.inLock
import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

/**
  * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading
  * (i.e. memory mapping) the underlying index until it is accessed for the first time via the
  * `get` method.
  *
  * In addition, this class exposes a number of methods (e.g. updateParentDir, renameTo, close,
  * etc.) that provide the desired behavior without causing the index to be loaded. If the index
  * had previously been loaded, the methods in this class simply delegate to the relevant method in
  * the index.
  *
  * This is an important optimization with regards to broker start-up and shutdown time if it has a
  * large number of segments.
  *
  * Methods of this class are thread safe. Make sure to check `AbstractIndex` subclasses
  * documentation to establish their thread safety.
  *
  * @param loadIndex A function that takes a `File` pointing to an index and returns a loaded
  *                  `AbstractIndex` instance.
  */
@threadsafe
class LazyIndex[T <: AbstractIndex] private (@volatile private var indexWrapper: IndexWrapper, loadIndex: File => T) {

  // mark 读写锁
  private val lock = new ReentrantLock()

  // mark 获取索引文件File对象
  def file: File = indexWrapper.file

  // mark 双重检查懒加载对应的索引文件信息 (只有在get的时候才会调用 loadIndex 加载真实索引)
  def get: T = {
    indexWrapper match {
      case indexValue: IndexValue[_] => indexValue.index.asInstanceOf[T]
      case _: IndexFile =>
        inLock(lock) {
          indexWrapper match {
            case indexValue: IndexValue[_] => indexValue.index.asInstanceOf[T]
            case indexFile: IndexFile =>
              // mark loadIndex 逻辑得看 new OffsetIndex(file, baseOffset, maxIndexSize, writable)
              // mark IndexValue.index实际上就是 new OffsetIndex(file, baseOffset, maxIndexSize, writable) 这个是真正的索引包装类
              /** 具体的逻辑参考 [[kafka.log.OffsetIndex]] [[kafka.log.AbstractIndex]] */
              val indexValue = new IndexValue(loadIndex(indexFile.file))
              indexWrapper = indexValue
              // 返回对应的Index对象
              indexValue.index
          }
        }
    }
  }

  def updateParentDir(parentDir: File): Unit = {
    inLock(lock) {
      indexWrapper.updateParentDir(parentDir)
    }
  }

  def renameTo(f: File): Unit = {
    inLock(lock) {
      indexWrapper.renameTo(f)
    }
  }

  def deleteIfExists(): Boolean = {
    inLock(lock) {
      indexWrapper.deleteIfExists()
    }
  }

  def close(): Unit = {
    inLock(lock) {
      indexWrapper.close()
    }
  }

  def closeHandler(): Unit = {
    inLock(lock) {
      indexWrapper.closeHandler()
    }
  }

}

object LazyIndex {

  def forOffset(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[OffsetIndex] = {
    // mark IndexFile 为索引文件的包装器
    // mark file => new OffsetIndex(file, baseOffset, maxIndexSize, writable) 是一个工厂函数 根据文件会生成OffsetIndex
    new LazyIndex(new IndexFile(file), file => new OffsetIndex(file, baseOffset, maxIndexSize, writable))
  }

  def forTime(file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true): LazyIndex[TimeIndex] =
    new LazyIndex(new IndexFile(file), file => new TimeIndex(file, baseOffset, maxIndexSize, writable))

  private sealed trait IndexWrapper {

    def file: File

    def updateParentDir(f: File): Unit

    def renameTo(f: File): Unit

    def deleteIfExists(): Boolean

    def close(): Unit

    def closeHandler(): Unit

  }

  /**
   * mark 索引文件对象的包装类 提供了针对索引文件.index结尾的文件操作
   *
   * @param _file 索引文件的初始文件对象
   */
  private class IndexFile(@volatile private var _file: File) extends IndexWrapper {

    /**
     * 获取索引文件对象。
     *
     * @return 索引文件对象
     */
    def file: File = _file

    /**
     * 更新索引文件的父目录。
     *
     * @param parentDir 新的父目录
     */
    def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

    /**
     * 将索引文件重命名为指定文件。
     *
     * @param f 新的文件对象
     */
    def renameTo(f: File): Unit = {
      try Utils.atomicMoveWithFallback(file.toPath, f.toPath, false)
      catch {
        case _: NoSuchFileException if !file.exists => ()
      }
      finally _file = f
    }

    /**
     * 如果索引文件存在，则删除它。
     *
     * @return 如果文件存在且被成功删除，则返回 true；否则返回 false
     */
    def deleteIfExists(): Boolean = Files.deleteIfExists(file.toPath)

    /**
     * 关闭索引文件。
     */
    def close(): Unit = ()

    /**
     * 关闭处理器。
     */
    def closeHandler(): Unit = ()
  }
  private class IndexValue[T <: AbstractIndex](val index: T) extends IndexWrapper {

    def file: File = index.file

    def updateParentDir(parentDir: File): Unit = index.updateParentDir(parentDir)

    def renameTo(f: File): Unit = index.renameTo(f)

    def deleteIfExists(): Boolean = index.deleteIfExists()

    def close(): Unit = index.close()

    def closeHandler(): Unit = index.closeHandler()

  }

}

