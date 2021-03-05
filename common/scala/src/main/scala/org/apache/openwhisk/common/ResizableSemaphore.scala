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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.AbstractQueuedSynchronizer
import scala.annotation.tailrec

/**
 * A Semaphore that has a specialized release process that optionally allows reduction of permits in batches.
 * When permit size after release is a factor of reductionSize, the release process will reset permits to state + 1 - reductionSize;
 * otherwise the release will reset permits to state + 1.
 * It also maintains an operationCount where a tryAquire + release is a single operation,
 * so that we can know once all operations are completed.
 *
 * @param maxAllowed    理解为当前invoker中已经启动的处理特定action的容器个数
 * @param reductionSize 理解为一个容器当中可以并发处理请求的数目
 *
 *    ResizableSemaphore主要是为了可并发执行的action(maxConcurrent > 1)设计的，主要目的就是分配action的并发资源
 *    ResizableSemaphore是NestedSemaphore的内部类，每个invoker有多少action，就对应创建多少个ResizableSemaphore，保存在map中，key就是action
 *
 */
class ResizableSemaphore(maxAllowed: Int, reductionSize: Int) {
  /**
   * 这个平平无奇的字段表示的是action的个数，其实action的个数等于( reductionSize * maxAllowed - State)
   * */
  private val operationCount = new AtomicInteger(0)
  /**
   * AQS是一个同步器，可以用来实现lock
   * 其暴露给用户的就是一个state变量和三个CAS函数（即三个函数不能并发）：
   *  setState：设置State
   *  getState：获取State
   *  compareAndSetState(expect,update)：若当前state==expect，那么将update赋值给state
   *
   *  AQS内部是用FIFO实现的，对于State的修改如果产生了竞争，那么后来的线程将会到队列中排队
   *
   * */
  class Sync extends AbstractQueuedSynchronizer {
    /**
     * ResizableSemaphore 是为每个action创建的，创建是在NestedSemaphore类的tryOrForceAcquireConcurrent函数中
     * 创建时maxAllowed都是0
     * state表示当前invoker的剩余可并发容量，初始化时为0
     * */
    setState(maxAllowed)

    def permits: Int = getState

    /** Try to release a permit and return whether or not that operation was successful.
     * release 显然指的是增加剩余可并发容量，releases只有两种情况，一种是新增一个容器，一种是请求实行完毕
     * 前者的参数应该是 reductionSize-1，后者的参数是 1
     * */
    @tailrec
    final def tryReleaseSharedWithResult(releases: Int): Boolean = {
      val current = getState
      val next2 = current + releases
      //这里做了双重判断，如果释放一个位置之后当前的并发容量可以被整除，也就意味着可以减少一个容器，因此next2还减去一个reductionSize
      //此函数的返回值有两个，第一个是释放后的剩余容量，第二个是是否要削减一个容器
      val (next, reduced) = if (next2 % reductionSize == 0) {
        (next2 - reductionSize, true)
      } else {
        (next2, false)
      }
      //next MIGHT be < current in case of reduction; this is OK!!!
      //这里使用compareAndSetState进行递归调用主要的目的就是保证一致性，因为State可能被修改
      if (compareAndSetState(current, next)) {
        reduced
      } else {
        tryReleaseSharedWithResult(releases)
      }
    }

    /**
     * Try to acquire a permit and return whether or not that operation was successful. Requests may not finish in FIFO
     * order, hence this method is not necessarily fair.
     */
    @tailrec
    final def nonFairTryAcquireShared(acquires: Int): Int = {
      val available = getState
      val remaining = available - acquires
      //这里如果是<0，那么将直接返回，需要注意的是，如果是remaining < 0返回的情况compareAndSetState(available, remaining)是不会被执行的
      //因为<0是一种错误，意味着剩余容量不足
      if (remaining < 0 || compareAndSetState(available, remaining)) {
        remaining
      } else {
        nonFairTryAcquireShared(acquires)
      }
    }
  }

  val sync = new Sync

  /**
   * Acquires the given numbers of permits.
   *
   * @param acquires the number of permits to get
   * @return `true`, iff the internal semaphore's number of permits is positive, `false` if negative
   */
  def tryAcquire(acquires: Int = 1): Boolean = {
    require(acquires > 0, "cannot acquire negative or no permits")
    if (sync.nonFairTryAcquireShared(acquires) >= 0) {
    //如果成功拿到位置，那么action就要被下放，从而operationCount++
      operationCount.incrementAndGet()
      true
    } else {
      false
    }
  }

  /**
   * Releases the given amount of permits
   *
   * @param acquires the number of permits to release
   * @return (releaseMemory, releaseAction) releaseMemory is true if concurrency count is a factor of reductionSize
   *         releaseAction is true if the operationCount reaches 0
   *         返回值，如果releaseMemory为true，那就就要释放内存空间，这里其实对应了Sync.tryReleaseSharedWithResult我们所说的多余出一个容器的情况;
   *                如果releaseMemory为true，那就要同时释放action的数据结构，也就是ResizableSemaphore本身
   */
  def release(acquires: Int = 1, opComplete: Boolean): (Boolean, Boolean) = {
    require(acquires > 0, "cannot release negative or no permits")
    //release always succeeds, so we can always adjust the operationCount
    /**
     * 参数opComplete其实对应了release的两种情况，在前面也说过了：
     *  1.一个action完成,此时operationCount需要减1,
     *  2.一个容器被创建，此时其实是action到来，由于容量不够导致的增加容器，从而导致整体容量增加，此时operationCount需要加1
     * */
    val releaseAction = if (opComplete) { // an operation completion
      //此时已经没有action了，因此此action对应的ResizableSemaphore可以被移除
      operationCount.decrementAndGet() == 0
    } else { //otherwise an allocation + operation initialization
      //加1到0是不可能发生的
      operationCount.incrementAndGet() == 0
    }
    (sync.tryReleaseSharedWithResult(acquires), releaseAction)
  }

  /** Returns the number of currently available permits. Possibly negative. */
  def availablePermits: Int = sync.permits

  //for testing
  def counter = operationCount.get()
}
