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
import akka.actor.ActorRefFactory
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.core.loadBalancer.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * A loadbalancer that schedules workload based on a hashing-algorithm.
 * 一种基于哈希算法调度工作负载的负载均衡器,也就是当一个请求到达时选择那个invoker的问题，在这里其目标是负载均衡
 *
 * ## Algorithm
 *
 * At first, for every namespace + action pair a hash is calculated and then an invoker is picked based on that hash
 * (`hash % numInvokers`). The determined index is the so called "home-invoker". This is the invoker where the following
 * progression will **always** start. If this invoker is healthy (see "Invoker health checking") and if there is
 * capacity on that invoker (see "Capacity checking"), the request is scheduled to it.
 * 首先，对于每个名称空间+动作对，都会计算一个哈希，然后根据该哈希选择一个调用程序（“hash％numInvokers”）。根据索引值确定的invoker称之为“home-invoker”。
 * 然后会检查这个invoker的healthy和capacity，如果都符合条件，那么就调度到这个invoker上去。
 *
 *
 * If one of these prerequisites is not true, the index is incremented by a step-size. The step-sizes available are the
 * all coprime numbers smaller than the amount of invokers available (coprime, to minimize collisions while progressing
 * through the invokers). The step-size is picked by the same hash calculated above (`hash & numStepSizes`). The
 * home-invoker-index is now incremented by the step-size and the checks (healthy + capacity) are done on the invoker
 * we land on now.
 * 如果这些先决条件之一不成立，那么索引将按步长递增。可用的步长大小包括了所有小于可使用的invoker数量（即互素，以最大程度地减少在invoker程序中发生的冲突）的互质数，
 * 而且步长自己也必须是质数（举个例子，如果当前可用的invoker为10,那么可用的步长就包括了1,3,7）。不知道怎地确定一个步长，然后就以home-invoker-index为基准向上递增
 *
 * This procedure is repeated until all invokers have been checked at which point the "overload" strategy will be
 * employed, which is to choose a healthy invoker randomly. In a steadily running system, that overload means that there
 * is no capacity on any invoker left to schedule the current request to.
 * 直至将所有的invoker检查一边，如果依然没有容量可用的，那么说明系统过载，此时则随机挑选一个healthy的invoker
 *
 * If no invokers are available or if there are no healthy invokers in the system, the loadbalancer will return an error
 * stating that no invokers are available to take any work. Requests are not queued anywhere in this case.
 * 如果是没有任何一个healthy的invoker，那么将返回错误
 *
 * An example:
 * - availableInvokers: 10 (all healthy)
 * - hash: 13
 * - homeInvoker: hash % availableInvokers = 13 % 10 = 3
 * - stepSizes: 1, 3, 7 (note how 2 and 5 is not part of this because it's not coprime to 10)
 * - stepSizeIndex: hash % numStepSizes = 13 % 3 = 1 => stepSize = 3
 *
 * Progression to check the invokers: 3, 6, 9, 2, 5, 8, 1, 4, 7, 0 --> done
 *
 * This heuristic is based on the assumption, that the chance to get a warm container is the best on the home invoker
 * and degrades the more steps you make. The hashing makes sure that all loadbalancers in a cluster will always pick the
 * same home invoker and do the same progression for a given action.
 * 上面的算法，可以看出，对于一个确定的action，它选择invoker的顺序基本上是确定的，这样其实是在最大程度的保证选择warm-container。而哈希策略则是最大程度的保证负载的均衡
 *
 * Known caveats（警告）:
 * - This assumption is not always true. For instance, two heavy workloads landing on the same invoker can override each
 * other, which results in many cold starts due to all containers being evicted by the invoker to make space for the
 * "other" workload respectively. Future work could be to keep a buffer of invokers last scheduled for each action and
 * to prefer to pick that one. Then the second-last one and so forth.
 * 其实上面的算法存在的问题也是容易理解的，因为hash毕竟是随机性的，如果两个调用频繁的invoker刚好被hash到一个invoker，那么就会频繁的替换彼此的warm-container，
 * 从而导致频繁冷启动
 *
 *
 * ## Capacity checking
 *
 * The maximum capacity per invoker is configured using `user-memory`, which is the maximum amount of memory of actions
 * running in parallel on that invoker.
 * 每个invoker的capacity是根据user-memory进行配置的，它表示了可以在同一个invoker中并行运行的最大值
 *
 * Spare capacity is determined by what the loadbalancer thinks it scheduled to each invoker. Upon scheduling, an entry
 * is made to update the books and a slot for each MB of the actions memory limit in a Semaphore is taken. These slots
 * are only released after the response from the invoker (active-ack) arrives **or** after the active-ack times out.
 * The Semaphore has as many slots as MBs are configured in `user-memory`.
 * 剩余capacity是由LB根据它的调度情况决定的。这里应该这么理解，invoker的mem按MB的大小分配一个slot与之对应，每当向invoker调度一个action时，就会
 * 根据action的memory limit减小相应的slot，这些被占据的slot仅仅在action返回或者超时后才会被释放，显然这里的slot仅仅表示内存的大小而非具体的位置
 *
 * Known caveats:
 * - In an overload scenario, activations are queued directly to the invokers, which makes the active-ack timeout
 * unpredictable. Timing out active-acks in that case can cause the loadbalancer to prematurely assign new load to an
 * overloaded invoker, which can cause uneven queues.
 * 超时机制在过载的情况下具有不可预测性，因为当出现过载时，action会在invoker上进行排队，但是这个排队的时间显然不算在action的执行时间当中的，
 * 这就有可能导致action仍然在执行，但是LB已经v把action所占据的slot给释放了
 * - The same is true if an invoker is extraordinarily slow in processing activations. The queue on this invoker will
 * slowly rise if it gets slow to the point of still sending pings, but handling the load so slowly, that the
 * active-acks time out. The loadbalancer again will think there is capacity, when there is none.
 * 这里的英语还是难以理解，大概的意思是LB发出activations后就会开始倒计时，但是invoker处理activations过于缓慢（比如网络拥堵，真正的action
 * 执行还没开始就timeout了），这里注意，LB仅仅发送的了activations，而action是需要invoker自己从DB中拉去
 *
 * Both caveats could be solved in future work by not queueing to invoker topics on overload, but to queue on a
 * centralized overflow topic. Timing out an active-ack can then be seen as a system-error, as described in the
 * following.active-ack超时可被视为系统错误
 *
 * ## Invoker health checking
 *
 * Invoker health is determined via a kafka-based protocol, where each invoker pings the loadbalancer every second. If
 * no ping is seen for a defined amount of time, the invoker is considered "Offline".
 * 调用者的health是通过基于kafka的协议确定的，每个调用者每秒都会对负载均衡器执行ping操作。 如果在定义的时间内没有发现ping，则将invoker视为“脱机”。
 *
 * Moreover, results from all activations are inspected. If more than 3 out of the last 10 activations contained system
 * errors, the invoker is considered "Unhealthy". If an invoker is unhealty, no user workload is sent to it, but
 * test-actions are sent by the loadbalancer to check if system errors are still happening. If the
 * system-error-threshold-count in the last 10 activations falls below 3, the invoker is considered "Healthy" again.
 * 此外，还会检查所有activations的结果。 如果最近10次激活中有3次以上包含系统错误，则将invoker视为“不健康”。 如果invoker运行不正常，则不会向用户发送任何
 * 工作负载，但是负载平衡器将发送测试操作，以检查是否仍在发生系统错误。 如果最近10次激活中的system-error-threshold-count低于3，
 * 则调用方再次被视为“健康”。
 *
 *
 * To summarize:
 * - "Offline": Ping missing for > 10 seconds 10秒内收不到ping视为脱机
 * - "Unhealthy": > 3 **system-errors** in the last 10 activations, pings arriving as usual  最近10次active-ack，超过3次超时视为不健康
 * - "Healthy": < 3 **system-errors** in the last 10 activations, pings arriving as usual    最近10次active-ack，不超过3次超时视为不健康
 *
 * ## Horizontal sharding
 *
 * Sharding is employed to avoid both loadbalancers having to share any data, because the metrics used in scheduling
 * are very fast changing.
 * 使用分片来避免两个负载均衡器必须共享任何数据，因为调度中使用的度量标准变化非常快。
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available (invoker-busy-threshold = 16), those will be divided to 8 slots for each loadbalancer
 * (if there are 2).
 * 水平分片意味着每个Invoker的capacity在负载均衡器之间平均分配。 如果调用者最多具有16个可用插槽（invoker-busy-threshold = 16），
 * 则每个负载均衡器会将这些插槽划分为8个插槽（如果有2个）。
 * 也就是如果存在多个LB，那么这些LB不要共享信息，各自管理invoker对应比例的资源
 *
 * If concurrent activation processing is enabled (and concurrency limit is > 1), accounting of containers and
 * concurrency capacity per container will limit the number of concurrent activations routed to the particular
 * slot at an invoker. Default max concurrency is 1.
 * 如果启用了并发处理，也就是一个容器实例，并发处理多个请求。那么容器数和容器的并发度将会限制路由到invoker特定slot的activations数目。
 *
 *
 * Known caveats:
 * - If a loadbalancer leaves or joins the cluster, all state is removed and created from scratch. Those events should
 * not happen often.
 * - If concurrent activation processing is enabled, it only accounts for the containers that the current loadbalancer knows.
 * So the actual number of containers launched at the invoker may be less than is counted at the loadbalancer, since
 * the invoker may skip container launch in case there is concurrent capacity available for a container launched via
 * some other loadbalancer.
 * 如果启用了并发激活处理，则仅考虑当前负载均衡器知道的容器。 因此，在调用程序上启动的容器的实际数量可能少于在负载平衡器上计算的数量，因为
 * 如果通过其他负载均衡器启动的容器有可用的并发容量，则调用程序可能会跳过容器启动。
 */
class ShardingContainerPoolBalancer(
                                     config: WhiskConfig,
                                     controllerInstance: ControllerInstanceId,
                                     feedFactory: FeedFactory,
                                     val invokerPoolFactory: InvokerPoolFactory,
                                     implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
                                     implicit actorSystem: ActorSystem,
                                     logging: Logging,
                                     materializer: ActorMaterializer)
  extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  /** Build a cluster of all loadbalancers */
  /**
   * 在分析一下  loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap 的语法：
   * ConfigKeys.cluster是字符串常量="whisk.cluster"
   * loadConfigOrThrow 首先接受了一个泛式参数，此参数也是返回值的类型，在这里也就是ClusterConfig
   * ClusterConfig是一个cass Class，并且拥有一个参数useClusterBootstrap
   * 我们在配置文件core/controller/src/main/resources/reference.conf中的确可以找到whisk.cluster的数据：{use-cluster-bootstrap: false}
   * 我估计这里是根据名字有对应关系，use-cluster-bootstrap直接赋值给了ClusterConfig类的参数useClusterBootstrap，也就是false
   * */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = ShardingContainerPoolBalancerState()(lbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ShardingContainerPoolBalancerState.updateInvokers]] and [[ShardingContainerPoolBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   * 这个monitor是用来更新invoker和cluster的状态的，采用了akka机制，这是一种基于消息的并发机制，每个执行单元称之为一个Actor，其主要作用和多线程并发
   * 类似，但是由于Actor只有在收到消息之后才会执行，因此不需要锁机制
   * 这里之所以使用akka机制，是为了保证updateInvokers和updateCluster不会同时执行
   *
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member) => availableMembers + member
          case ReachableMember(member) => availableMembers + member
          case MemberRemoved(member, _) => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _ => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)

  override def clusterSize: Int = schedulingState.clusterSize

  /** 1. Publish a message to the loadbalancer */
  /** 这里是实现了trait LoadBalancer中的publish方法 */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    logging.info(this, "KINGDO: publish")
    logging.info(this, "KINGDO" + schedulingState.toString)

    //action.exec.pull表示是否需要注册表中的容器映像来执行action，也就是否是blackbox，即用户自定义镜像
    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    //获取可用的Invoker列表和步长
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
    //chosen的类型是Option
    val chosen = if (invokersToUse.nonEmpty) {
      //计算Action的Hash值
      val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
      //计算次Action的homeInvoker索引
      val homeInvoker = hash % invokersToUse.size
      //确定步长
      val stepSize = stepSizes(hash % stepSizes.size)
      //这里Option的类型是元组，元组有两个元素，一个是选择的Invoker，一个是选择过程是否是force
      val invoker: Option[(InvokerInstanceId, Boolean)] = ShardingContainerPoolBalancer.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes,
        homeInvoker,
        stepSize)
      invoker.foreach {
        //如果过载,那就记录一下。。。。
        case (_, true) =>
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      //在这里map中应该接一个函数，将元组转化成一个对象，这里直接采用了原则的数字索引 ._1 返回地一个元素
      //因此这里的返回结果就是Some(InvokerInstanceId)，也就是选择的Invoker
      invoker.map(_._1)
    } else {
      None
    }
    //Option.map操作，是将Option中的元素取出后执行函数，且函数的参数类型和Option的类型一致，并将返回结果保存到Option
    chosen
      .map { invoker =>
        //到了这里已经把invoker选择出来了，接下来要发消息给他
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) {
          "std"
        } else {
          "non-std"
        }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) {
          "std"
        } else {
          "non-std"
        }
        logging.info(
          this,
          s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")

        /**
         *  设置activationSlots、activationPromises以记录action的调用信息
         *  返回值就是publish的返回值
         *  就是添加了ack超时的定时器
         * */
        val activationResult = setupActivation(msg, action, invoker)
        /**
         * 一层层向上找，可以知道发送消息的接口是由KafkaMessagingProvider实现的
         * 这里解释把msg直接推到invoker的订阅上，接下来就是invoker接受到消息了
         * */
        /**
         * 需要注意的是在这里我们将给出publish的返回值，返回值类型是 Future[ Future[ Either[ActivationId, WhiskActivation] ] ],这是一个组合的Future，
         * 也就是内部的Future返回后，外部的Future才可能返回
         * 这里直接使用了map进行组内，
         *    外面的Future就是send的返回值Future[RecordMetadata]，这个
         *    里面的Future是activationResult，类型是Future[ Either[ActivationId, WhiskActivation] ]
         * 需要知道这两个都是由Promise产生的，外面的是只要send操作完成就会，Future就有效了，里面的只能等到结果返回（如果是非阻塞调用，那么也是立即返回的）
         * */
        sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        //如果chosen为空，那么将会执行这段代码
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  /**
   * createInvokerPool返回的是ActorRef对象，可以给monitor发送消息
   * */
  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  /**
   * invoker,类型是InvokerInstanceId，InvokerInstanceId和InvokerState共同组成了InvokerHealth,InvokerHealth就是schedulingState中invokers字段的Seq成员类型
   * entry，类型是ActivationEntry，包含了Action的全套信息
   * 功能就是把释放指定Invoker中的指定Action
   * */
  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    //A.lift(x)操作等价于Some(A(x))
    //lift的返回值是Option,显然在这里只能返回一个或者None，需要注意的是Option表示的就是有或者没有，有的话就是Some，没有的话就是None，因此是0和1的选择
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
  }
}

object ShardingContainerPoolBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
                                      actorRefFactory: ActorRefFactory,
                                      messagingProvider: MessagingProvider,
                                      messagingProducer: MessageProducer,
                                      sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
                                      monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor))
      }

    }
    new ShardingContainerPoolBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized.
   * 返回参数X的互素数(小于X，且自身必须是质数)，保存到索引序列中
   * 如果是10的话返回：0->1,1->3,2->7 size=3
   * 如果是10的话返回: size=0
   * */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * schedule函数的逻辑非常简单，就是根据之前的算法在可用的Invoker当中选择一个容量可用的，选择的顺序是根据homeIndex和步长来决定，如果没有那么就随机
   * 找一个容量够的，因此有三种返回结果：
   * Some(invoker，false) 可用且容量充足的
   * Some(invoker，true)  随机选择的（容量不足）
   * None                 无可用
   * 而且一旦作出选择，NestedSemaphore的state就更新了
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers      a list of available invokers to search in, including their state
   * @param dispatched    semaphores for each invoker to give the slots away from
   * @param slots         Number of slots, that need to be acquired (e.g. memory in MB)
   * @param index         the index to start from (initially should be the "homeInvoker"
   * @param step          stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  @tailrec
  def schedule(
                maxConcurrent: Int,
                fqn: FullyQualifiedEntityName,
                invokers: IndexedSeq[InvokerHealth],
                dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                slots: Int,
                index: Int,
                step: Int,
                stepsDone: Int = 0)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
    val numInvokers = invokers.size

    if (numInvokers > 0) {
      val invoker = invokers(index)
      //test this invoker - if this action supports concurrency, use the scheduleConcurrent function
      if (invoker.status.isUsable && dispatched(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
        Some(invoker.id, false)
      } else {
        // If we've gone through all invokers
        if (stepsDone == numInvokers + 1) {
          val healthyInvokers = invokers.filter(_.status.isUsable)
          if (healthyInvokers.nonEmpty) {
            // Choose a healthy invoker randomly
            val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
            dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
            logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
            Some(random, true)
          } else {
            None
          }
        } else {
          val newIndex = (index + step) % numInvokers
          schedule(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1)
        }
      }
    } else {
      None
    }
  }
}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers          all of the known invokers in the system
 * @param _managedInvokers   all invokers for managed runtimes
 * @param _blackboxInvokers  all invokers for blackbox runtimes
 * @param _managedStepSizes  the step-sizes possible for the current managed invoker count
 * @param _blackboxStepSizes the step-sizes possible for the current blackbox invoker count
 * @param _invokerSlots      state of accessible slots of each invoker
 */
case class ShardingContainerPoolBalancerState(
                                               //IndexedSeq是索引序列，许对元素的随机和快速访问，而Seq是线性序列，允许快速访问仅第一个和最后一个元素，其余按顺序访问
                                               //InvokerHealth 包含了invokerId和invokerState
                                               private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth], /*这里的empty意味着分配一个空的序列*/
                                               private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                               private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                               private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0), /*这里参数依然是不知道具体的invoker个数的因此步长也是空*/
                                               private var _blackboxStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
                                               protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
                                               IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
                                               private var _clusterSize: Int = 1)(
                                               lbConfig: ShardingContainerPoolBalancerConfig =
                                               loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  // Managed fraction and blackbox fraction的取值是独立的，范围的都是0.0-1.0,也就是两者之和是0.0-2.0
  // 如果两者之和是1.0 那么两者是不存在重叠的（当然如果N太小另当别论），如果两者之和是2.0 也就意味着两者没有区别
  // 如果两者之和小于1.0 那么会适当增加blackbox fraction以与managed fraction互补
  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers

  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers

  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers

  def managedStepSizes: Seq[Int] = _managedStepSizes

  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes

  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots

  def clusterSize: Int = _clusterSize

  /**
   * @param memory
   * @return calculated invoker slot
   *         注意这里是给定invoker的ByteSize然后做一个简单的计算
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    /**
     * 这里的cluster可以理解为由多个LB构成的内部集群，因为在分片LB的设计当中每个LB平均得到invoker的内存资源
     * */
    val invokerShardMemorySize = memory / _clusterSize
    /**
     * 内存的大小限制在配置文件：common/scala/src/main/resources/application.conf中
     * memory { min = 128 m max = 512 m std = 256 m }
     * */
    val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newTreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   * Invoker的更新一般发生在初始化的时候，这是个不能经常执行的操作
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    /*原invoker的个数*/
    val newSize = newInvokers.size /*现invoker的个数*/

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    // Math.ceil是向上取整而Math.floor是像下取整，在N比较小的时候managed invokers的数目是可以覆盖blackbox invokers的，也就是说基本上在
    // 小于10个的时候managed是全部的invoker
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    //从左边取出managed个invoker
    _managedInvokers = _invokers.take(managed)
    //从右边取出blackboxes个invoker，从这里也可以看出，虽然有保证不重叠的想法，但是并不是特别严格
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    val logDetail = if (oldSize != newSize) {
      // 重新计算步长
      _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)
      // 仅仅考虑新增invoker节点的情况，因为小于原来的话应该定义为offline
      if (oldSize < newSize) {
        // Keeps the existing state..
        //此时_invokerSlots仅仅包含以前invoker的NestedSemaphore
        //Seq的drop操作是舍弃前面n个并返回新列表，因此得到onlyNewInvokers就是新添加的invoker，这里就是要为这些invoker新建NestedSemaphore
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        //这里的语法是，遍历onlyNewInvokers中的元素，将每个invoker元素做参数调用匿名函数，并将返回值形成新的队列（也就是NestedSemaphore队列）
        // 然后添加到_invokerSlots中，这里的=>就是匿名函数，invoker就是参数
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          //NestedSemaphore对象需要传入Invoker的内存大小(MB为单位),getInvokerSlot是根据invoker的真实物理内存再根据cluster大小计算的
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB}")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   *
   * 这个操作实际上是抛弃了原来所有的状态，这么做真的没问题吗？？
   *
   */
  /**
   * 这里的cluster可以理解为由多个LB构成的内部集群，因为在分片LB的设计当中每个LB平均得到invoker的内存资源
   * */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      //这里是在重新的计算每个invoker的内存分配
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size - total invoker memory ${totalInvokerMemory}.")(
        TransactionId.loadbalancer)
    }
  }
}

/**
 * Configuration for the cluster created between loadbalancers.
 *
 * @param useClusterBootstrap Whether or not to use a bootstrap mechanism
 */
case class ClusterConfig(useClusterBootstrap: Boolean)

/**
 * Configuration for the sharding container pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 *                         openwhisk 针对不同的语言环境有自己的官方镜像，用户也可以自己定义容器镜像，称之为blackbox,blackbox不能使用所有的invoker，只能使用总数的1/10，
 *                         这个比例称之为blackboxFraction
 * @param timeoutFactor    factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 *                         factor to increase the timeout for forced active acks
 *                         default is 2 because init and run can both use the configured timeout fully
 * @param timeoutAddon     extra time to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 *                         extra time to increase the timeout for forced active acks
 *                         default is 1.minute
 *
 *                         以上三个参数均在配置文件core/controller/src/main/resources/reference.conf里设置
 */
case class ShardingContainerPoolBalancerConfig(managedFraction: Double,
                                               blackboxFraction: Double,
                                               timeoutFactor: Int,
                                               timeoutAddon: FiniteDuration)

/**
 * State kept for each activation slot until completion.
 * 保存了Action的全套信息
 *
 * @param id                       id of the activation
 * @param namespaceId              namespace that invoked the action
 * @param invokerName              invoker the action is scheduled to
 * @param memoryLimit              memory limit of the invoked action
 * @param timeLimit                time limit of the invoked action
 * @param maxConcurrent            concurrency limit of the invoked action
 * @param fullyQualifiedEntityName fully qualified name of the invoked action
 * @param timeoutHandler           times out completion of this activation, should be canceled on good paths
 * @param isBlackbox               true if the invoked action is a blackbox action, otherwise false (managed action)
 * @param isBlocking               true if the action is invoked in a blocking fashion, i.e. "somebody" waits for the result
 */
case class ActivationEntry(id: ActivationId,
                           namespaceId: UUID,
                           invokerName: InvokerInstanceId,
                           memoryLimit: ByteSize,
                           timeLimit: FiniteDuration,
                           maxConcurrent: Int,
                           fullyQualifiedEntityName: FullyQualifiedEntityName,
                           timeoutHandler: Cancellable,
                           isBlackbox: Boolean,
                           isBlocking: Boolean)
