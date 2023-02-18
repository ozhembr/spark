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

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

import org.apache.spark.SparkContext
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, ThreadUtils}


abstract class ParallelTaskScheduler(
   val sc: SparkContext,
   factory: (SparkContext, Int, Boolean, Clock,
     Option[Int], () => HealthTracker) => TaskSchedulerImpl)
  extends TaskScheduler with Logging {
  val isLocal = false
  val clock = new SystemClock
  val parallelism = sc.conf.get("spark.driver.schedulers.parallelism").toInt
  val maxTaskFailures = sc.conf.get(config.TASK_MAX_FAILURES)

  val executorTracker = new ExecutorTracker(parallelism)

  lazy val schedulerAssignmentManager: SchedulerAssignmentManager =
    SchedulerAssignmentManager(
      sc.conf,
      parallelism,
      dagScheduler,
      executorTracker,
      innerTaskSchedulers(_).busyExecutors
    )(clock)

  private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor {
    val n = sc.conf.getOption("spark.driver.schedulers.task.threads").map(_.toInt).getOrElse(20)

    ThreadUtils.newDaemonFixedThreadPool(n, "parallel-task-scheduler")
  }

  private val taskSupport = new ExecutionContextTaskSupport(executionContext)

  val stageIdToTaskSchedulerId: mutable.Map[Int, Int] = new ConcurrentHashMap[Int, Int]().asScala

  var schedulerBackend: ParallelSchedulerBackend = _

  val innerTaskSchedulers: immutable.IndexedSeq[TaskSchedulerImpl] =
    (0 until parallelism).map { i =>
      factory(sc, maxTaskFailures, isLocal, clock, Some(i), () => schedulerBackend.healthTracker)
    }

  val parallelInnerTaskSchedulers: ParSeq[TaskSchedulerImpl] = {
    val par = innerTaskSchedulers.par

    par.tasksupport = taskSupport

    par
  }

  override val schedulingMode: SchedulingMode = SchedulingMode.FIFO

  // TODO combine rootPools from inner schedulers
  override val rootPool: Pool =
    new Pool("parallel-task-scheduler", schedulingMode, 0, 0)

  private var dagScheduler: DAGScheduler = _

  private def healthTracker: ParallelHealthTracker = schedulerBackend.healthTracker

  private def taskSchedulerByStageId(stageId: Int): TaskSchedulerImpl = {
    innerTaskSchedulers(stageIdToTaskSchedulerId(stageId))
  }

  private def taskSchedulerByExecutorId(executorId: String): Option[TaskSchedulerImpl] = {
    executorTracker
      .indexByExecutorId(executorId)
      .map(innerTaskSchedulers(_))
  }

  override def initialize(backend: SchedulerBackend): Unit = {
    schedulerBackend = backend.asInstanceOf[ParallelSchedulerBackend]
  }

  override def start(): Unit = {
    schedulerBackend.start()
    innerTaskSchedulers.foreach(_.start())
  }

  override def stop(): Unit = {
    schedulerBackend.stop()
    innerTaskSchedulers.foreach(_.stop())
  }

  override def submitTasks(taskSet: TaskSet): Unit = {
    val jobId = taskSet.priority

    val schedulerId = schedulerAssignmentManager.assignTasks(taskSet)

    logInfo(s"Submit tasks to scheduler=$schedulerId" +
      s" jobId=$jobId stageId=${taskSet.stageId} tasks=${taskSet.tasks.length}")

    stageIdToTaskSchedulerId += taskSet.stageId -> schedulerId

    Future {
      innerTaskSchedulers(schedulerId).submitTasks(taskSet)
    }
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {
    stageIdToTaskSchedulerId
      .remove(stageId)
      .foreach { id =>
        Future {
          innerTaskSchedulers(id).cancelTasks(stageId, interruptThread)
        }
      }
  }

  override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    parallelInnerTaskSchedulers.exists(_.killTaskAttempt(taskId, interruptThread, reason))
  }

  override def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit = {
    stageIdToTaskSchedulerId
      .remove(stageId)
      .foreach { id =>
        Future {
          innerTaskSchedulers(id).killAllTaskAttempts(stageId, interruptThread, reason)
        }
      }
  }

  override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
    // TODO descrease scheduler load
    logDebug(s"notifyPartitionCompletion($stageId, $partitionId)")
    taskSchedulerByStageId(stageId).notifyPartitionCompletion(stageId, partitionId)
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler

    innerTaskSchedulers.foreach(_.setDAGScheduler(dagScheduler))
  }

  override def defaultParallelism(): Int = {
    innerTaskSchedulers.head.defaultParallelism()
  }

  override def executorHeartbeatReceived(
    execId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId,
    executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    taskSchedulerByExecutorId(execId) match {
      case Some(s) =>
        s.executorHeartbeatReceived(execId, accumUpdates, blockManagerId, executorUpdates)
      case _ =>
        logDebug(s"no scheduler for executor $execId")
        true
    }
  }

  override def executorDecommission(
    executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit = {
    taskSchedulerByExecutorId(executorId)
      .foreach(_.executorDecommission(executorId, decommissionInfo))
  }

  override def getExecutorDecommissionState(
    executorId: String): Option[ExecutorDecommissionState] = {
    taskSchedulerByExecutorId(executorId)
      .flatMap(_.getExecutorDecommissionState(executorId))
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    executorTracker.removeExecutor(executorId)
      .foreach(innerTaskSchedulers(_).executorLost(executorId, reason))
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    dagScheduler.workerRemoved(workerId, host, message)
  }

  override def applicationAttemptId(): Option[String] = None

  override def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    // scheduler with assigned executor is updated via StatusUpdate event
  }

  override def getExecutorsAliveOnHost(host: String): Option[Set[String]] = {
    logDebug(s"getExecutorsAliveOnHost host=$host")

    val par = executorTracker
      .indexesByHost(host)
      .toSeq
      .map(innerTaskSchedulers(_))
      .par

    par.tasksupport = taskSupport

    par
      .map(_.getExecutorsAliveOnHost(host))
      .foldLeft(Option.empty[Set[String]]) {
        case (Some(set), Some(result)) =>
          Option(set ++ result)
        case (acc, None) => acc
        case (_, result) => result
      }
  }

  override def excludedNodes(): Set[String] = {
    healthTracker.excludedNodeList()
  }

  // called by inner scheduler
  override def resourceOffers(
    offers: IndexedSeq[WorkerOffer], isAllFreeResources: Boolean): Seq[Seq[TaskDescription]] = {
    logInfo(s"Resource offers=${offers.size} isAllFreeResources=$isAllFreeResources")
    Seq()
  }

  override def taskSetManagerByTaskId(taskId: Long): Option[TaskSetManager] = {
    logDebug(s"taskSetManagerByTaskId id=$taskId")

    // accessing concurrent map
    innerTaskSchedulers.map(_.taskSetManagerByTaskId(taskId)).find(_.isDefined).flatten
  }

  override def isExecutorBusy(execId: String): Boolean = {
    logDebug(s"isExecutorBusy id=$execId")

    executorTracker
      .indexByExecutorId(execId)
      .exists(innerTaskSchedulers(_).isExecutorBusy(execId))
  }

  override def onCompletionEvent(event: CompletionEvent): Unit = {
    schedulerAssignmentManager.onCompletionEvent(event)
  }

  override def onJobSuccess(jobId: Int): Unit = {
    schedulerAssignmentManager.onJobSuccess(jobId)
  }

  override def onJobFailure(jobId: Int): Unit = {
    schedulerAssignmentManager.onJobFailure(jobId)
  }
}
