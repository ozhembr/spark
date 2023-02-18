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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.immutable

import org.apache.spark.{SparkConf, TaskFailedReason}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor
import org.apache.spark.util.{Clock, ThreadUtils}

class SchedulerAssignmentManager(
    conf: SparkConf,
    parallelism: Int,
    dagSchedulerState: DAGSchedulerState,
    executorTracker: ExecutorTracker,
    schedulerBusyExecutors: Int => immutable.Set[String])(implicit clock: Clock) extends Logging {

  // count of tasks per scheduler
  private[parallel] val schedulerLoads: IndexedSeq[SchedulerLoad] = {
    (0 until parallelism).map(new SchedulerLoad(_))
  }
  private[parallel] val jobIdToSchedulerId = new ConcurrentHashMap[Int, (Int, Set[Int])]()
  private[parallel] val stageIdToSchedulerId = new ConcurrentHashMap[Int, Int]()

  case class ReassignCandidate(time: Long, schedulerId: Int, executorId: String)
  case class ReassignCandidates(executorIds: Set[String], candidates: Vector[ReassignCandidate]) {
    def add(time: Long, schedulerId: Int, executorId: String): ReassignCandidates = {
      if (executorIds(executorId)) {
        this
      } else {
        ReassignCandidates(
          executorIds + executorId,
          candidates :+ ReassignCandidate(time, schedulerId, executorId)
        )
      }
    }

    def get(timeLimit: Long): Seq[ReassignCandidate] = {
      candidates.takeWhile(_.time < timeLimit)
    }

    def drop(timeLimit: Long): ReassignCandidates = {
      val (left, right) = candidates.partition(_.time < timeLimit)

      val set = executorIds -- left.map(_.executorId)

      ReassignCandidates(set, right)
    }
  }
  object ReassignCandidates {
    val empty: ReassignCandidates = ReassignCandidates(Set(), Vector())
  }

  private val executorCandidates =
    new AtomicReference[ReassignCandidates](ReassignCandidates.empty)

  private val reassignExecutorService = {
    val period = conf.getOption("spark.sam.reassign.period").map(_.toInt).getOrElse(1000)

    val t = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "scheduler-assignment-manager-reassign")

    t.scheduleAtFixedRate(() => {
      val time = clock.getTimeMillis()

      val schedulerCandidates =
        executorCandidates.getAndUpdate(_.drop(time))
          .get(time)
          .groupBy(_.schedulerId)
          .filter(_._2.nonEmpty)

      if (schedulerCandidates.nonEmpty) {
        logInfo(s"Reassign candidates: ${schedulerCandidates.mkString("\n")}")

        loadIncreaseLock.synchronized {
          schedulerCandidates.foreach {
            case (schedulerId, seq) =>
              executorTracker.extraExecutors(
                schedulerId,
                seq.map(_.executorId),
                schedulerLoads(schedulerId).total,
                schedulerBusyExecutors(schedulerId),
                starvingSchedulers(schedulerId, _))
          }
        }
      }
    }, period, period, TimeUnit.MILLISECONDS)

    t
  }

  private val loadIncreaseLock = new Object

  class SchedulerLoad(val schedulerId: Int) {
    val stageIdToTaskNum = new ConcurrentHashMap[Int, Int]()
    val _total = new AtomicInteger()

    def add(stageId: Int, taskNum: Int): Unit = {
      stageIdToTaskNum.put(stageId, taskNum)
      _total.addAndGet(taskNum)
    }

    def decreaseLoad(stageId: Int, taskNum: Int): Int = {
      val stageTasks = stageIdToTaskNum.compute(stageId, (_, current) => current - taskNum)
      if (stageTasks < 1) {
        logDebug(s"decreaseLoad if stageId=$stageId stageTasks=$stageTasks taskNum=$taskNum")
      }

      val n = _total.addAndGet(-taskNum)

      if (n < 1) {
        logDebug(s"decreaseLoad stageId=$stageId taskNum=$taskNum n=$n")
      }

      n
    }

    def removeStage(stageId: Int): Option[Int] = {
      val taskNumOpt = Option(stageIdToTaskNum.remove(stageId))

      taskNumOpt
        .filter(_ > 0)
        .foreach { taskNum =>
          val n = _total.addAndGet(-taskNum)

          logDebug(s"removeStage stageId=$stageId taskNum=$taskNum n=$n")
        }

      taskNumOpt
    }

    def total: Int = _total.get()
  }

  implicit val loadOrdering: Ordering[SchedulerLoad] = {
    Ordering.by[SchedulerLoad, Int](_.total)
  }

  // get same scheduler for job tasks or assign the least loaded scheduler for new job
  def assignTasks(taskSet: TaskSet): Int = {
    loadIncreaseLock.synchronized {
    val jobId = taskSet.priority
    val stageId = taskSet.stageId
    val tasksNum = taskSet.tasks.length

    val schedulerId =
      jobIdToSchedulerId
        .compute(jobId, (_, tuple) => {
          if (tuple == null) {
            schedulerLoads.min.schedulerId -> Set(taskSet.stageId)
          } else {
            val (schedulerId, stages) = tuple

            schedulerId -> (stages + taskSet.stageId)
          }
        })._1

    val schedulerLoad = schedulerLoads(schedulerId)

    stageIdToSchedulerId.put(stageId, schedulerId)

    schedulerLoad.add(stageId, tasksNum)

    // executorCount <= schedulerLoad.total
    executorTracker.unstashExecutor(schedulerId, tasksNum)

    logInfo(s"Assign job=$jobId stage=$stageId to scheduler=$schedulerId" +
      s" load=${schedulerLoad.total}")

    schedulerId
  }}

  // head scheduler with the highest load
  private[parallel] def starvingSchedulers(
    skipScheduler: Int, schedulerExecutorsCount: IndexedSeq[Int]): Seq[(Int, Int)] = {
    schedulerLoads
      .view
      .filter(_.schedulerId != skipScheduler)
      .map { schedulerLoad =>
        val load = schedulerLoad.total

        val schedulerId = schedulerLoad.schedulerId
        val count = schedulerExecutorsCount(schedulerId)

        val loadCoef =
          if (count == 0) {
            load.toDouble
          } else if (load <= count) {
            0f
          } else {
            load.toDouble / count
          }

        logDebug(s"Load check scheduler=$schedulerId load=$load count=$count coef=$loadCoef")

        (schedulerId, loadCoef, load - count)
      }
      .sortBy(_._2)(Ordering[Double].reverse)
      .collect {
        case (id, _, starveNum) if starveNum > 0 =>
          id -> starveNum
      }
      .toVector
  }

  // use 2 locks
  def assignExecutor(registerExecutor: RegisterExecutor,
                     callerAddr: RpcAddress): Option[Int] = {
  loadIncreaseLock.synchronized {
    executorTracker
      .addExecutor(
        registerExecutor,
        callerAddr,
        starvingSchedulers(-1, _).headOption.map(_._1))
  }}

  def onCompletionEvent(event: CompletionEvent): Unit = {
    val stageId = event.task.stageId
    var keepExecutor = true

    event.reason match {
      case tfr: TaskFailedReason =>
        logError(s"TaskFailedReason: ${tfr.toErrorString}")
      case _ =>
        dagSchedulerState.stageIdToStage.get(stageId)
          .map(_.jobIds.toSeq) // release concurrent
          .filter(_.nonEmpty)
          .foreach { jobIds =>
            if (jobIds.size > 1) {
              logError(s"Multiple jobs=${jobIds.mkString(",")} for " +
                s"stageId=$stageId taskId=${event.taskInfo.taskId}")
            }

            jobIds
              .foreach { jobId =>
                Option(jobIdToSchedulerId.get(jobId))
                  .foreach {
                    case (schedulerId, _) =>
                      val schedulerLoad = schedulerLoads(schedulerId)
                      val load = schedulerLoad.decreaseLoad(stageId, 1)

                      // TODO check single executor for multiple jobs/schedulers
                      if (keepExecutor) {
                        val count = executorTracker.schedulerExecutorsCount(schedulerId)

                        if (count > load) {
                          keepExecutor = false

                          val executorId = event.taskInfo.executorId

                          logDebug(s"onTaskEnd schedulerId=$schedulerId executorId=$executorId" +
                            s" load=$load count=$count")

                          val time = clock.getTimeMillis()

                          executorCandidates.updateAndGet(_.add(time, schedulerId, executorId))
                        }
                      }
                  }
              }
          }
    }
  }

  def onJobFinish(jobId: Int, isSuccess: Boolean): Unit = {
    logInfo(s"Job($jobId) isSuccess=$isSuccess")

    Option(jobIdToSchedulerId.remove(jobId))
      .foreach {
        case (schedulerId, stages) =>
          // submission of new tasks is blocked by lock,
          // so it is safe to release idle executors from scheduler
          // and new executors won't be assigned if it has extra
          loadIncreaseLock.synchronized {
            val schedulerLoad = schedulerLoads(schedulerId)

            stages.foreach(schedulerLoad.removeStage)

            val load = schedulerLoad.total
            val count = executorTracker.schedulerExecutorsCount(schedulerId)

            if (count > load) {
              executorTracker.extraExecutors(
                schedulerId,
                load,
                schedulerBusyExecutors(schedulerId),
                starvingSchedulers(schedulerId, _))
            }
          }
      }
  }

  def onJobFailure(jobId: Int): Unit = onJobFinish(jobId, false)

  def onJobSuccess(jobId: Int): Unit = onJobFinish(jobId, true)
}

object SchedulerAssignmentManager {
  def apply(conf: SparkConf,
            parallelism: Int,
            dagScheduler: DAGScheduler,
            executorTracker: ExecutorTracker,
            schedulerBusyExecutors: Int => immutable.Set[String])
           (implicit clock: Clock)
  : SchedulerAssignmentManager = {
    val manager =
      new SchedulerAssignmentManager(
        conf, parallelism, dagScheduler, executorTracker, schedulerBusyExecutors)

    manager
  }
}
