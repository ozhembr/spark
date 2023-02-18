/*
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

package org.apache.spark.scheduler.parallel

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.immutable
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

import org.apache.spark._
import org.apache.spark.executor.ExecutorLogUrlHandler
import org.apache.spark.internal.config.UI
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcAddress, RpcCallContext, RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, ExecutorData}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

trait ParallelSchedulerBackend extends SchedulerBackend with SchedulerBackendCommon
  with ExecutorTracker.Callbacks with ExecutorAllocationClient {
  import ParallelSchedulerBackend._

  val parallelTaskScheduler: ParallelTaskScheduler

  override def taskScheduler: TaskScheduler = parallelTaskScheduler

  private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor {
    val n = sc.conf.getOption("spark.driver.schedulers.backend.threads").map(_.toInt).getOrElse(20)

    ThreadUtils.newDaemonFixedThreadPool(n, "parallel-scheduler-backend")
  }

  private val taskSupport = new ExecutionContextTaskSupport(executionContext)

  lazy val healthTracker: ParallelHealthTracker = new ParallelHealthTracker(sc, this)

  override val driverEndpoint: RpcEndpointRef = rpcEnv.setupEndpoint(
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME, new DriverEndpoint)

  lazy val logUrlHandler: ExecutorLogUrlHandler = new ExecutorLogUrlHandler(
    conf.get(UI.CUSTOM_EXECUTOR_LOG_URL))

  lazy val schedulerAssignmentManager: SchedulerAssignmentManager =
    parallelTaskScheduler.schedulerAssignmentManager

  val executorTracker: ExecutorTracker = {
    val tracker = parallelTaskScheduler.executorTracker
    tracker.setCallbacks(this)
    tracker
  }

  val taskSchedulers: immutable.IndexedSeq[TaskSchedulerImpl] =
    parallelTaskScheduler.innerTaskSchedulers

  val innerSchedulers: immutable.IndexedSeq[CoarseGrainedSchedulerBackend] = {
    val sparkContext = sc

    taskSchedulers.zipWithIndex.map {
      case (ts, i) =>
        val backend =
          new CoarseGrainedSchedulerBackend {
            override def sc: SparkContext = sparkContext
            override def taskScheduler: TaskScheduler = ts
            override def id: Option[Int] = Some(i)

            override def start(): Unit = {}

            override protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
              ParallelSchedulerBackend.this.doKillExecutors(executorIds)
            }
          }

        ts.initialize(backend)

        backend
    }
  }

  lazy val parallelInnerSchedulers: ParSeq[CoarseGrainedSchedulerBackend] = {
    val par = innerSchedulers.par

    par.tasksupport = taskSupport

    par
  }

  lazy val innerSchedulersEndpoints: immutable.IndexedSeq[RpcEndpointRef] =
    innerSchedulers.map(_.driverEndpoint)

  val currentExecutorIdCounter = new AtomicInteger()

  private val _numLocalityAwareTasksPerResourceProfileId = new AtomicReference[Map[Int, Int]](Map())

  private val _hostToLocalTaskCount = new AtomicReference[Map[Int, Map[String, Int]]](Map())

  private def schedulerByExecutorId(executorId: String): Option[CoarseGrainedSchedulerBackend] = {
    executorTracker.indexByExecutorId(executorId).map(innerSchedulers(_))
  }

  class DriverEndpoint extends IsolatedRpcEndpoint {
    override val rpcEnv: RpcEnv = sc.env.rpcEnv

    def endpointByExecutorId(executorId: String): Option[RpcEndpointRef] = {
      executorTracker
        .indexByExecutorId(executorId)
        .map { schedulerId =>
          logDebug(s"endpointByExecutorId executorId=$executorId at schedulerId=$schedulerId")

          innerSchedulersEndpoints(schedulerId)
        }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case statusUpdate: StatusUpdate =>
        endpointByExecutorId(statusUpdate.executorId).foreach(_.send(statusUpdate))

      case ReviveOffers =>
        innerSchedulersEndpoints.foreach(_.send(ReviveOffers))

      case killTask: KillTask =>
        endpointByExecutorId(killTask.executor).foreach(_.send(killTask))

      case killExecutorsOnHost @ KillExecutorsOnHost(host) =>
        executorTracker.removeHost(host)
          .foreach(innerSchedulersEndpoints(_).send(killExecutorsOnHost))

      case UpdateDelegationTokens(tokens) =>
        updateDelegationTokens(tokens)

      case removeExecutor @ RemoveExecutor(executorId, _) =>
        logDebug(s"ParallelScheduler $removeExecutor")

        executorTracker
          .removeExecutor(executorId)
          .foreach { i =>
            innerSchedulersEndpoints(i)
              .send(removeExecutor)
          }

      case removeWorker: RemoveWorker => // TODO
        logInfo(removeWorker.toString)

      case launchedExecutor: LaunchedExecutor =>
        endpointByExecutorId(launchedExecutor.executorId).foreach(_.send(launchedExecutor))

      case m =>
        logError(s"Unknown message $m")
    }

    /**
     * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
     * `SparkException` will be thrown and sent to `onError`.
     */
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case registerExecutor @ RegisterExecutor(executorId, executorRef, hostname, cores, logUrls,
      attributes, resources, resourceProfileId) =>
        val executorAddress = if (executorRef.address != null) {
          executorRef.address
        } else {
          context.senderAddress
        }

        schedulerAssignmentManager
          .assignExecutor(registerExecutor, context.senderAddress)
          .foreach { schedulerId =>
            currentExecutorIdCounter.accumulateAndGet(
              executorId.toInt,
              (acc, v) => {
                if (acc < v) {
                  v
                } else {
                  acc
                }
              })

            logInfo(
              s"Register executor with scheduler=$schedulerId" +
                s" id=$executorId address=$executorAddress cores=$cores")
            innerSchedulersEndpoints(schedulerId).ask(registerExecutor)

            val resourcesInfo = resources.map {
              case (rName, info) =>
                // tell the executor it can schedule resources up to numSlotsPerAddress times,
                // as configured by the user, or set to 1 as that is the default (1 task/resource)
                val numParts = sc.resourceProfileManager
                  .resourceProfileFromId(resourceProfileId).getNumSlotsPerAddress(rName, conf)
                (info.name, new ExecutorResourceInfo(info.name, info.addresses, numParts))
            }

            val data = new ExecutorData(executorRef, executorAddress, hostname,
              0, cores, logUrlHandler.applyPattern(logUrls, attributes), attributes,
              resourcesInfo, resourceProfileId, System.currentTimeMillis())

            listenerBus.post(
              SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          }

        context.reply(true)

      case StopDriver =>
        context.reply(true)

        innerSchedulersEndpoints.foreach(_.ask(StopDriver))

      case StopExecutors =>
        innerSchedulersEndpoints.foreach(_.ask(StopExecutors))

        context.reply(true)

      case removeWorker: RemoveWorker => // self message for inner scheduler
        context.reply(true)

      case executorDecommissioning @ ExecutorDecommissioning(executorId) =>
        context.reply {
          endpointByExecutorId(executorId)
            .exists(_.ask(executorDecommissioning).await(defaultAskTimeout))
        }

      case RetrieveSparkAppConfig(resourceProfileId) =>
        val rp = sc.resourceProfileManager.resourceProfileFromId(resourceProfileId)
        val reply = SparkAppConfig(
          sparkProperties,
          SparkEnv.get.securityManager.getIOEncryptionKey(),
          Option(delegationTokens.get()),
          rp)
        context.reply(reply)

      case isExecutorAlive @ IsExecutorAlive(executorId) =>
        context.reply {
          endpointByExecutorId(executorId)
            .exists(_.ask(isExecutorAlive).await(defaultAskTimeout))
        }

      case e =>
        logError(s"Received unexpected ask $e")
    }

    override def onConnected(remoteAddress: RpcAddress): Unit = {
      logDebug(s"onConnected $remoteAddress")
    }

    override def onError(cause: Throwable): Unit = {
      logError("onError", cause)
    }

    override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
      logError(s"onNetworkError $remoteAddress", cause)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      logDebug(s"ParallelSchedulerBackend onDisconnected $remoteAddress")

      executorTracker
        .removeByAddress(remoteAddress)
        .foreach {
          case (executorId, _) => onDisconnectedExecutor(remoteAddress, executorId)
        }
    }

    override def onStop(): Unit = {
      logDebug("onStop")
    }

    override def onStart(): Unit = {
      logDebug("onStart")
    }
  }

  override def stop(): Unit = {
    logDebug("stop()")
    delegationTokenManager.foreach(_.stop())
    innerSchedulers.foreach(_.stop())
  }

  override def reviveOffers(): Unit = {
    logDebug("reviveOffers")
  }

  override def defaultParallelism(): Int = innerSchedulers.head.defaultParallelism()

  override def maxNumConcurrentTasks(rp: ResourceProfile): Int =
    parallelInnerSchedulers.map(_.maxNumConcurrentTasks(rp)).max // TODO mappings?

  private lazy val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  override def lastAllocatedExecutorId: Int = currentExecutorIdCounter.get()

  override def requestTotalExecutors(
                             resourceProfileIdToNumExecutors: Map[Int, Int],
                             numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
                             hostToLocalTaskCount: Map[Int, Map[String, Int]]
                           ): Boolean = {
    val resourceProfileToNumExecutors = resourceProfileIdToNumExecutors.map { case (rpid, num) =>
      (sc.resourceProfileManager.resourceProfileFromId(rpid), num)
    }

    this._numLocalityAwareTasksPerResourceProfileId.set(numLocalityAwareTasksPerResourceProfileId)
    this._hostToLocalTaskCount.set(hostToLocalTaskCount)

    doRequestTotalExecutors(
      resourceProfileToNumExecutors,
      numLocalityAwareTasksPerResourceProfileId,
      hostToLocalTaskCount)
      .await(defaultAskTimeout)
  }

  override def updateDelegationTokens(tokens: Array[Byte]): Unit = {
    delegationTokens.set(tokens)

    val message = UpdateDelegationTokens(tokens)
    innerSchedulersEndpoints.foreach(_.send(message))
  }

  override private[spark] def getExecutorIds(): Seq[String] = {
    executorTracker.executorIds
  }

  override def isExecutorActive(executorId: String): Boolean = {
    schedulerByExecutorId(executorId)
      .exists(_.isExecutorActive(executorId))
  }

  override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    doRequestTotalExecutors(
      Map(sc.resourceProfileManager.defaultResourceProfile -> executorTracker.totalExecutors),
      _numLocalityAwareTasksPerResourceProfileId.get(),
      _hostToLocalTaskCount.get()
    ).await(defaultAskTimeout)
  }

  override def killExecutors(executorIds: Seq[String],
                             adjustTargetNumExecutors: Boolean,
                             countFailures: Boolean,
                             force: Boolean): Seq[String] = {
    logDebug(s"killExecutors(${executorIds.sorted.mkString(",")})")

    val execsByScheduler = executorTracker.groupExecutorsByScheduler(executorIds).par

    execsByScheduler.tasksupport = taskSupport

    execsByScheduler
      .map {
        case (execIds, schedulerId) =>
          innerSchedulers(schedulerId)
            .killExecutors(execIds, adjustTargetNumExecutors, countFailures, force)
      }
      .foldLeft(Seq[String]())(_ ++ _)
  }

  override def killExecutorsOnHost(host: String): Boolean = {
    driverEndpoint.send(KillExecutorsOnHost(host))
    true
  }

  override private[scheduler] def disableExecutor(executorId: String): Boolean = {
    schedulerByExecutorId(executorId)
      .exists(_.disableExecutor(executorId))
  }

  override def reassignExecutors(
    oldSchedulerId: Int, seq: Seq[RegisteredExecutor], nextSchedulerId: Int): Seq[String] = {
    logDebug(s"Reassign executors from schedulerId=$oldSchedulerId" +
      s" seq=${seq.map(_.executorId)}, nextSchedulerId=$nextSchedulerId")

    if (seq.nonEmpty) {
      val clearedExecutors =
        innerSchedulers(oldSchedulerId)
          .isolatedRpcEndpoint
          .clearExecutors(seq.map(_.executorId))

      val executorSet = clearedExecutors.toSet

      seq.filter(re => executorSet(re.executorId))
        .foreach(innerSchedulersEndpoints(nextSchedulerId).send)

      clearedExecutors
    } else {
      Seq()
    }
  }

  override def suspendExecutors(schedulerId: Int, executorIds: Seq[String]): Seq[String] = {
    logDebug(s"suspendExecutors schedulerId=$schedulerId executorIds=$executorIds")

    if (executorIds.nonEmpty) {
      innerSchedulers(schedulerId)
        .isolatedRpcEndpoint
        .clearExecutors(executorIds)
    } else {
      Seq()
    }
  }

  override def assignExecutors(schedulerId: Int, seq: Seq[RegisteredExecutor]): Unit = {
    logDebug(s"assignExecutors schedulerId=$schedulerId seq=${seq.map(_.executorId)}")

    if (seq.nonEmpty) {
      innerSchedulersEndpoints(schedulerId)
        .send(RegisteredExecutors(seq))
    }
  }

  override def getTotalRegisteredExecutors(): Int = executorTracker.totalExecutors

  override def onDisconnectedExecutor(rpcAddress: RpcAddress, executorId: String): Unit = {}
}

object ParallelSchedulerBackend {
  implicit class FutureOpt[A](val future: Future[A]) extends AnyVal {
    def await(timeout: RpcTimeout): A = timeout.awaitResult(future) // TODO replace with callback
  }
}
