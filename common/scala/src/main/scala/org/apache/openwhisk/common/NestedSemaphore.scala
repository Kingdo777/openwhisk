/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.common

import scala.collection.concurrent.TrieMap

/**
 * A Semaphore that coordinates the memory (ForcibleSemaphore) and concurrency (ResizableSemaphore) where
 * - for invocations when maxConcurrent == 1, delegate to super
 * - for invocations that cause acquire on memory slots, also acquire concurrency slots, and do it atomically
 *
 * @param memoryPermits
 * @tparam T 这里的T是一个范式参数，即可以接受任意类型的参数，在这里T代表的是Action
 *
 * 每一个invoker都有一个NestedSemaphore对象用于管理资源
 * NestedSemaphore继承于ForcibleSemaphore，用于管理内存资源
 * NestedSemaphore内部有一个名为actionConcurrentSlotsMap的Map，包含了与可并发Action一一对应的ResizableSemaphore，用于管理并发资源
 *
 *
 */
class NestedSemaphore[T](memoryPermits: Int) extends ForcibleSemaphore(memoryPermits) {
  //ResizableSemaphore是NestedSemaphore的内部类，每个invoker有多少action，就对应创建多少个ResizableSemaphore，保存在map中，key就是action
  //在这里map使用的是TrieMap，key使用了泛式参数，其实就是FullyQualifiedEntityName
  private val actionConcurrentSlotsMap = TrieMap.empty[T, ResizableSemaphore] //one key per action; resized per container

  final def tryAcquireConcurrent(actionid: T, maxConcurrent: Int, memoryPermits: Int): Boolean = {

    if (maxConcurrent == 1) {
      /**
       * 如果是不并发的Action，那么只需要直接申请内存资源即可，如果内存充足就会成功，否则失败
       * */
      super.tryAcquire(memoryPermits)
    } else {
      tryOrForceAcquireConcurrent(actionid, maxConcurrent, memoryPermits, false)
    }
  }

  /**
   * Coordinated permit acquisition:
   * - first try to acquire concurrency slot
   * - then try to acquire lock for this action
   * - within the lock:
   *     - try to acquire concurrency slot (double check)
   *     - try to acquire memory slot
   *     - if memory slot acquired, release concurrency slots
   *       - release the lock
   *       - if neither concurrency slot nor memory slot acquired, return false
   *
   * @param actionid
   * @param maxConcurrent
   * @param memoryPermits
   * @param force
   * @return
   */
  private def tryOrForceAcquireConcurrent(actionid: T,
                                          maxConcurrent: Int,
                                          memoryPermits: Int,
                                          force: Boolean): Boolean = {
    //首先判断此action是否有对应的ResizableSemaphore对象
    val concurrentSlots = actionConcurrentSlotsMap
      .getOrElseUpdate(actionid, new ResizableSemaphore(0, maxConcurrent))
    //首先尝试获取并发资源，如果成功则直接返回
    if (concurrentSlots.tryAcquire(1)) {
      true
    } else {
      // with synchronized:
      //这里的用法类似与Java中的this.synchronized，表示这段代码在多线程中是不能同步执行的
      concurrentSlots.synchronized {
        //再次尝试，因为前面的代码是可同步的，因此可能在多线程中可能这次try又成功了
        if (concurrentSlots.tryAcquire(1)) {
          true
          //如果依然失败，则检查是否是强制分配
        } else if (force) {
          //如果是的话，那么直接分配内存资源，然后通过release增加并发资源
          super.forceAcquire(memoryPermits)
          concurrentSlots.release(maxConcurrent - 1, false)
          true
          //否则的则要尝试分配资源
        } else if (super.tryAcquire(memoryPermits)) {
          //如果成功，那么通过release增加并发资源
          concurrentSlots.release(maxConcurrent - 1, false)
          //如果失败，那么就要返回false
          true
        } else {
          false
        }
      }
    }
  }

  /**
   * 这里是强制的申请资源，因此对于不并发直接调用forceAcquire，对于并发的要设置force=true的参数
   * */
  def forceAcquireConcurrent(actionid: T, maxConcurrent: Int, memoryPermits: Int): Unit = {
    require(memoryPermits > 0, "cannot force acquire negative or no permits")
    if (maxConcurrent == 1) {
      super.forceAcquire(memoryPermits)
    } else {
      tryOrForceAcquireConcurrent(actionid, maxConcurrent, memoryPermits, true)
    }
  }

  /**
   * Releases the given amount of permits
   *
   * @param acquires the number of permits to release
   */
  /**
   * 其实已经非常的明确：
   *   对于非并发而言，直接调用force的release即可
   *   对于并发，先调用Resizeable的release，然后根据返回结果再决定是否要释放内存以及Action
   *
   * */
  def releaseConcurrent(actionid: T, maxConcurrent: Int, memoryPermits: Int): Unit = {
    require(memoryPermits > 0, "cannot release negative or no permits")
    if (maxConcurrent == 1) {
      super.release(memoryPermits)
    } else {
      val concurrentSlots = actionConcurrentSlotsMap(actionid)
      val (memoryRelease, actionRelease) = concurrentSlots.release(1, true)
      //concurrent slots
      if (memoryRelease) {
        super.release(memoryPermits)
      }
      if (actionRelease) {
        actionConcurrentSlotsMap.remove(actionid)
      }
    }
  }

  //for testing
  def concurrentState = actionConcurrentSlotsMap.readOnlySnapshot()
}
