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

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorSystem
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Abstract class which provides common logic for all LoadBalancer implementations.
 */
abstract class CommonLoadBalancer(config: WhiskConfig,
                                  feedFactory: FeedFactory,
                                  controllerInstance: ControllerInstanceId)(implicit val actorSystem: ActorSystem,
                                                                            logging: Logging,
                                                                            materializer: ActorMaterializer,
                                                                            messagingProvider: MessagingProvider)
  extends LoadBalancer {

  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer)
  protected val invokerPool: ActorRef

  /** State related to invocations and throttling */
  protected[loadBalancer] val activationSlots = TrieMap[ActivationId, ActivationEntry]()
  protected[loadBalancer] val activationPromises =
    TrieMap[ActivationId, Promise[Either[ActivationId, WhiskActivation]]]()
  /**
   * 关于LongAdder：
   * 文档解释,LongAdder是由一个或者多个变量维护的一个初始为0的和，每个线程都可以通过add()函数更新和的大小，LongAdder的变量集会根据
   * 并发激烈程度来动态的增减变量的个数，通过sum()函数将返回和的大小
   * LongAdder在高并发的场景下拥有很高的效率，可以用于计算qps
   * */
  protected val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationMemory = new LongAdder()
  protected val totalManagedActivationMemory = new LongAdder()

  protected def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LOADBALANCER_ACTIVATIONS_INFLIGHT(controllerInstance), totalActivations.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, ""),
      totalBlackBoxActivationMemory.longValue + totalManagedActivationMemory.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, "Blackbox"),
      totalBlackBoxActivationMemory.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, "Managed"),
      totalManagedActivationMemory.longValue)
  }

  actorSystem.scheduler.schedule(10.seconds, 10.seconds)(emitMetrics())

  override def activeActivationsFor(namespace: UUID): Future[Int] =
    Future.successful(activationsPerNamespace.get(namespace).map(_.intValue).getOrElse(0))

  override def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue)

  /**
   * Calculate the duration within which a completion ack must be received for an activation.
   * 用于计算收到任务完成的ack的超时时间
   * Calculation is based on the passed action time limit. If the passed action time limit is shorter than
   * the configured standard action time limit, the latter is used to avoid too tight timeouts.
   * 这个时间的计算是基于action自身的时间i限制的，但是如果次限制小于标准时限（1m），那么就按照标准时间限制来计算，避免这个时间过短
   * The base timeout is multiplied with a configurable timeout factor. This dilution(稀释) controls how much slack(富裕、闲置) you
   * want to allow in your system before you start reporting failed activations. The default value of 2 bases
   * on invoker behavior that a cold invocation's init duration may be as long as its run duration. Higher factors
   * may account for additional wait times.
   *
   * Finally, a configurable duration is added to the diluted timeout to be lenient towards general delays / wait times.
   *
   * @param actionTimeLimit the action's time limit
   * @return the calculated time duration within which a completion ack must be received
   */
  private def calculateCompletionAckTimeout(actionTimeLimit: FiniteDuration): FiniteDuration = {
    (actionTimeLimit.max(TimeLimit.STD_DURATION) * lbConfig.timeoutFactor) + lbConfig.timeoutAddon
  }

  /**
   * 2. Update local state with the activation to be executed scheduled.
   *
   * All activations are tracked in the activationSlots map. Additionally, blocking invokes
   * are tracked in the activationPromises map. When a result is received via result ack, it
   * will cause the result to be forwarded to the caller waiting on the result, and cancel
   * the DB poll which is also trying to do the same.
   * Once the completion ack arrives, activationSlots entry will be removed.
   *
   * 所有的activations都会被activationSlots追踪，此外对于阻塞调用(wsk action invoke -blocking)会被activationPromises map追踪
   * 当收到结果的ack之后。会将结果发给等待结果的调用者，同时取消对DB的轮循
   * 一旦接受到action完成的ack，对应的activationSlots将被删除
   *
   * 这个函数的主要作用就是
   * 设置activationSlots、activationPromises以记录action的调用信息
   * 返回值就是publish接口的返回值
   * 最后就是添加了ack超时的定时器
   */
  protected def setupActivation(msg: ActivationMessage,
                                action: ExecutableWhiskActionMetaData,
                                instance: InvokerInstanceId): Future[Either[ActivationId, WhiskActivation]] = {

    /**
     * 初始化4个LongAdder的私有变量
     * */
    // Needed for emitting metrics.
    totalActivations.increment()
    val isBlackboxInvocation = action.exec.pull
    val totalActivationMemory =
      if (isBlackboxInvocation) totalBlackBoxActivationMemory else totalManagedActivationMemory
    totalActivationMemory.add(action.limits.memory.megabytes)

    activationsPerNamespace.getOrElseUpdate(msg.user.namespace.uuid, new LongAdder()).increment()

    /**
     * 计算任务完成的超时时间
     * */
    // Completion Ack must be received within the calculated time.
    val completionAckTimeout = calculateCompletionAckTimeout(action.limits.timeout.duration)

    // If activation is blocking, store a promise that we can mark successful later on once the result ack
    // arrives. Return a Future representing the promise to caller.
    // If activation is non-blocking, return a successfully completed Future to caller.
    /**
     * 这里用到了future的另一种创建方法，之前是通过创建Future对象来创建，这样创建的future可以认为是一个只读的占位符
     * future还可以通过Promise创建，并且我们可以通过Promise来给future进行赋值
     * Promise.future 返回Future对象f
     * Promise.success 给f返回一个成功的值，即可以被on_success回调
     * Promise.failure 给f返回一个failure的值，即可以被on_failure回调
     *
     * Future.successful是直接返回一个带success的future对象，也就是返回后可以直接获取值，返回值类型是Either[A,B]类型，而且是Left，也就是A
     *
     * 因此resultPromise分两种情况：
     * 如果是blocking的，那么返回一个由Promise创建的future对象，次future的值需要等到结果返回之后再由Promise赋予
     * 如果不是blocking的，那么结果是确定的，因此直接返回了带有success值的future对象
     * 此外由于两种情况的返回值不同，因此使用了Either[A,B]
     *
     */
    val resultPromise = if (msg.blocking) {
      activationPromises.getOrElseUpdate(msg.activationId, Promise[Either[ActivationId, WhiskActivation]]()).future
    } else Future.successful(Left(msg.activationId))

    // Install a timeout handler for the catastrophic case where a completion ack is not received at all
    // (because say an invoker is down completely, or the connection to the message bus is disrupted) or when
    // the completion ack is significantly delayed (possibly dues to long queues but the subject should not be penalized);
    // in this case, if the activation handler is still registered, remove it and update the books.
    /**
     * 在这里设置了一个定时器，以处理ack超时，超时后直接调用processCompletion
     * 无论是否正常的收到了ack都会调用processCompletion函数，区别在于超时后参数forced的值是true
     * */
    // Attention: a significantly delayed completion ack means that the invoker is still busy or will be busy in future
    // with running the action. So the current strategy of freeing up the activation's memory in invoker
    // book-keeping will allow the load balancer to send more activations to the invoker. This can lead to
    // invoker overloads so that activations need to wait until other activations complete.
    activationSlots.getOrElseUpdate(
      msg.activationId, {
        val timeoutHandler = actorSystem.scheduler.scheduleOnce(completionAckTimeout) {
          processCompletion(msg.activationId, msg.transid, forced = true, isSystemError = false, instance = instance)
        }

        // please note: timeoutHandler.cancel must be called on all non-timeout paths, e.g. Success
        ActivationEntry(
          msg.activationId,
          msg.user.namespace.uuid,
          instance,
          action.limits.memory.megabytes.MB,
          action.limits.timeout.duration,
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          timeoutHandler,
          isBlackboxInvocation,
          msg.blocking)
      })

    resultPromise
  }

  protected val messageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))

  /** 3. Send the activation to the invoker */
  protected def sendActivationToInvoker(producer: MessageProducer,
                                        msg: ActivationMessage,
                                        invoker: InvokerInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid
    //订阅了topic的invoker就会收到消息，看来每个人的订阅的格式就是  invoker${invoker.toInt}
    val topic = s"invoker${invoker.toInt}"
    //下面两句主要是统计
    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}'",
      logLevel = InfoLevel)

    producer.send(topic, msg).andThen {
      //消息发出后,send将返回一个Future
      //这里依然是做log记录，对应于前面的start，进行时间的统计
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
          logLevel = InfoLevel)
      case Failure(_) => transid.failed(this, start, s"error on posting to topic $topic")
    }
  }

  /** Subscribes to ack messages from the invokers (result / completion) and registers a handler for these messages. */
  /**
   * 追根溯源
   * 首先Controller会调用ShardingContainerPoolBalancer.instance创建LB，在创建LB是传递了feedFactory参数，也就是createFeedFactory()函数，
   * 此函数在是由ShardingContainerPoolBalancer继承的trait LoadBalancerProvider实现的函数,该函数返回i一个FeedFactory对象，此对象也就是传给LB的参数，
   * 也就是下面的feedFactory，FeedFactory实现了一个函数，也就是feedFactory.createFeed,此函数最终创建了一个MessageFeed对象,MessageFeed对象会轮循消息管道
   * 并通过传入的handler进行处理，也就是processAcknowledgement，从而构成一个闭环.
   *
   * MessageFeed集成自FSM集成自Actor,因此MessageFeed还可以处理Actor的消息，processAcknowledgement中每处理一次消息，就会执行activationFeed ! MessageFeed.Processed
   *
   * 因此消息的接收是在创建LB的时候就已经创建好的了
   *
   * */
  private val activationFeed: ActorRef =
    feedFactory.createFeed(actorSystem, messagingProvider, processAcknowledgement)

  /** 4. Get the ack message and parse it */
  /**
   * 此函数返回Future参数，主要用于接受invoker的反馈消息
   * */
  protected[loadBalancer] def processAcknowledgement(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    AcknowledegmentMessage.parse(raw) match {
      /** 消息类型： AcknowledegmentMessage * */
      case Success(acknowledegment) =>
        acknowledegment.isSlotFree.foreach { instance =>
          processCompletion(
            acknowledegment.activationId,
            acknowledegment.transid,
            forced = false,
            isSystemError = acknowledegment.isSystemError.getOrElse(false),
            instance)
        }

        acknowledegment.result.foreach { response =>
          processResult(acknowledegment.activationId, acknowledegment.transid, response)
        }

        activationFeed ! MessageFeed.Processed

      case Failure(t) =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw")

      case _ =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"Unexpected Acknowledgment message received by loadbalancer: $raw")
    }
  }


  /**
   * 需要注意一点，在这里我们的有两个ack：
   * result ack：这个仅仅使用于阻塞调用的情况，ack中包含了返回结果
   * completion ack：指的是action结束，主要用于释放资源
   * */

  /** 5. Process the result ack and return it to the user */
  protected def processResult(aid: ActivationId,
                              tid: TransactionId,
                              response: Either[ActivationId, WhiskActivation]): Unit = {
    // Resolve the promise to send the result back to the user.
    // The activation will be removed from the activation slots later, when the completion message
    // is received (because the slot in the invoker is not yet free for new activations).
    activationPromises.remove(aid).foreach(_.trySuccess(response))
    logging.info(this, s"received result ack for '$aid'")(tid)
  }

  /** 功能就是把释放指定Invoker中的指定Action * */
  protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit

  // Singletons for counter metrics related to completion acks
  protected val LOADBALANCER_COMPLETION_ACK_REGULAR =
    LoggingMarkers.LOADBALANCER_COMPLETION_ACK(controllerInstance, RegularCompletionAck)
  protected val LOADBALANCER_COMPLETION_ACK_FORCED =
    LoggingMarkers.LOADBALANCER_COMPLETION_ACK(controllerInstance, ForcedCompletionAck)
  protected val LOADBALANCER_COMPLETION_ACK_HEALTHCHECK =
    LoggingMarkers.LOADBALANCER_COMPLETION_ACK(controllerInstance, HealthcheckCompletionAck)
  protected val LOADBALANCER_COMPLETION_ACK_REGULAR_AFTER_FORCED =
    LoggingMarkers.LOADBALANCER_COMPLETION_ACK(controllerInstance, RegularAfterForcedCompletionAck)
  protected val LOADBALANCER_COMPLETION_ACK_FORCED_AFTER_REGULAR =
    LoggingMarkers.LOADBALANCER_COMPLETION_ACK(controllerInstance, ForcedAfterRegularCompletionAck)

  /** 6. Process the completion ack and update the state */
  protected[loadBalancer] def processCompletion(aid: ActivationId,
                                                tid: TransactionId,
                                                forced: Boolean,
                                                isSystemError: Boolean,
                                                instance: InstanceId): Unit = {

    val invoker = instance match {
      case i: InvokerInstanceId => Some(i)
      case _ => None
    }

    val invocationResult = if (forced) {
      InvocationFinishedResult.Timeout
    } else {
      // If the response contains a system error, report that, otherwise report Success
      // Left generally is considered a Success, since that could be a message not fitting into Kafka
      if (isSystemError) {
        InvocationFinishedResult.SystemError
      } else {
        InvocationFinishedResult.Success
      }
    }

    activationSlots.remove(aid) match {
      case Some(entry) =>
        totalActivations.decrement()
        val totalActivationMemory =
          if (entry.isBlackbox) totalBlackBoxActivationMemory else totalManagedActivationMemory
        totalActivationMemory.add(entry.memoryLimit.toMB * (-1))
        activationsPerNamespace.get(entry.namespaceId).foreach(_.decrement())

        invoker.foreach(releaseInvoker(_, entry))

        if (!forced) {
          entry.timeoutHandler.cancel()
          // notice here that the activationPromises is not touched, because the expectation is that
          // the active ack is received as expected, and processing that message removed the promise
          // from the corresponding map
          logging.info(this, s"received completion ack for '$aid', system error=$isSystemError")(tid)

          MetricEmitter.emitCounterMetric(LOADBALANCER_COMPLETION_ACK_REGULAR)

        } else {
          // the entry has timed out; if the active ack is still around, remove its entry also
          // and complete the promise with a failure if necessary
          activationPromises
            .remove(aid)
            .foreach(_.tryFailure(new Throwable("no completion or active ack received yet")))
          val actionType = if (entry.isBlackbox) "blackbox" else "managed"
          val blockingType = if (entry.isBlocking) "blocking" else "non-blocking"
          val completionAckTimeout = calculateCompletionAckTimeout(entry.timeLimit)
          logging.warn(
            this,
            s"forced completion ack for '$aid', action '${entry.fullyQualifiedEntityName}' ($actionType), $blockingType, mem limit ${entry.memoryLimit.toMB} MB, time limit ${entry.timeLimit.toMillis} ms, completion ack timeout $completionAckTimeout from $instance")(
            tid)

          MetricEmitter.emitCounterMetric(LOADBALANCER_COMPLETION_ACK_FORCED)
        }

        // Completion acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
        // guard this
        invoker.foreach(invokerPool ! InvocationFinishedMessage(_, invocationResult))
      case None if tid == TransactionId.invokerHealth =>
        // Health actions do not have an ActivationEntry as they are written on the message bus directly. Their result
        // is important to pass to the invokerPool because they are used to determine if the invoker can be considered
        // healthy again.
        logging.info(this, s"received completion ack for health action on $instance")(tid)

        MetricEmitter.emitCounterMetric(LOADBALANCER_COMPLETION_ACK_HEALTHCHECK)

        // guard this
        invoker.foreach(invokerPool ! InvocationFinishedMessage(_, invocationResult))
      case None if !forced =>
        // Received a completion ack that has already been taken out of the state because of a timeout (forced ack).
        // The result is ignored because a timeout has already been reported to the invokerPool per the force.
        // Logging this condition as a warning because the invoker processed the activation and sent a completion
        // message - but not in time.
        logging.warn(
          this,
          s"received completion ack for '$aid' from $instance which has no entry, system error=$isSystemError")(tid)

        MetricEmitter.emitCounterMetric(LOADBALANCER_COMPLETION_ACK_REGULAR_AFTER_FORCED)
      case None =>
        // The entry has already been removed by a completion ack. This part of the code is reached by the timeout and can
        // happen if completion ack and timeout happen roughly at the same time (the timeout was triggered before the completion
        // ack canceled the timer). As the completion ack is already processed we don't have to do anything here.
        logging.debug(this, s"forced completion ack for '$aid' which has no entry")(tid)

        MetricEmitter.emitCounterMetric(LOADBALANCER_COMPLETION_ACK_FORCED_AFTER_REGULAR)
    }
  }
}
