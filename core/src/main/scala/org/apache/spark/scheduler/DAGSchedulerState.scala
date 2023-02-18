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

package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentHashMap

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, Set}
import scala.collection.mutable

import org.apache.spark.util.ThreadUtils

trait DAGSchedulerState { this: DAGScheduler =>
  private val lock = ThreadUtils.readWriteLock()

  private var _jobIdToStageIds = Map[Int, Set[Int]]()
  private var _stageIdToStage: mutable.Map[Int, Stage] =
    new ConcurrentHashMap[Int, Stage]().asScala
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   */
  private var _shuffleIdToStageMap = Map[Int, ShuffleMapStage]()

  private var _jobIdToActiveJob = Map[Int, ActiveJob]()

  // Stages we need to run whose parents aren't done
  private var _waitingStages = Set[Stage]()

  // Stages we are running right now
  private var _runningStages = Set[Stage]()

  // Stages that must be resubmitted due to fetch failures
  private var _failedStages = Set[Stage]()

  private var _activeJobs = Set[ActiveJob]()

  def jobIdToStageIds: Map[Int, Set[Int]] = {
    lock.read(_jobIdToStageIds)
  }

  def stageIdToStage: collection.Map[Int, Stage] = _stageIdToStage

  def shuffleIdToStageMap: Map[Int, ShuffleMapStage] = {
    lock.read(_shuffleIdToStageMap)
  }

  def jobIdToActiveJob: Map[Int, ActiveJob] = {
    lock.read(_jobIdToActiveJob)
  }

  def waitingStages: Set[Stage] = {
    lock.read(_waitingStages)
  }

  def runningStages: Set[Stage] = {
    lock.read(_runningStages)
  }

  def failedStages: Set[Stage] = {
    lock.read(_failedStages)
  }

  def activeJobs: Set[ActiveJob] = {
    lock.read(_activeJobs)
  }

  protected def stageInfos(jobId: Int): Array[StageInfo] = lock.read {
    _jobIdToStageIds(jobId)
      .toArray
      .flatMap(id => _stageIdToStage.get(id).map(_.latestInfo))
  }

  protected def isNotInProcess(stage: Stage): Boolean = lock.read {
    !_waitingStages(stage) && !_runningStages(stage) && !_failedStages(stage)
  }

  protected def updateState(
      stages: List[Stage],
      shuffleIdStagePairs: Seq[(Int, ShuffleMapStage)]): Unit = {
    if (stages.nonEmpty || shuffleIdStagePairs.nonEmpty) {
      lock.write {
        stages.foreach { stage =>
          addStage(stage.firstJobId, stage)
        }

        shuffleIdStagePairs.foreach {
          case (shuffleId, stage) =>
            addShuffleStage(stage.firstJobId, stage, shuffleId)
        }
      }
    }
  }

  protected def addActiveJob(job: ActiveJob): Unit = lock.write {
    _jobIdToActiveJob += job.jobId -> job
    _activeJobs += job
  }

  protected def addWaitingStages(stages: Stage*): Unit = lock.write {
    _waitingStages ++= stages
  }

  protected def addRunningStages(stages: Stage*): Unit = lock.write {
    _runningStages ++= stages
  }

  protected def addFailedStages(stages: Stage*): Unit = lock.write {
    _failedStages ++= stages
  }

  protected def removeJobStages(job: ActiveJob, stages: List[Stage]): Unit = lock.write {
    _jobIdToStageIds -= job.jobId
    _jobIdToActiveJob -= job.jobId
    _activeJobs -= job

    stages.foreach { stage =>
      val newRunningStages = _runningStages - stage

      if (_runningStages.size != newRunningStages.size) {
        _runningStages = newRunningStages
      }

      val newWaitingStages = _waitingStages - stage

      if (_waitingStages.size != newWaitingStages.size) {
        _waitingStages = newWaitingStages
      }

      val newFailedStages = _failedStages - stage

      if (_failedStages.size != newFailedStages.size) {
        _failedStages = newFailedStages
      }

      _stageIdToStage -= stage.id
    }
  }

  protected def removeWaitingStages(childStages: Stage*): Unit = lock.write {
    _waitingStages --= childStages
  }

  protected def removeRunningStages(stages: Stage*): Unit = lock.write {
    _runningStages --= stages
  }

  protected def clearFailedStages(): Unit = lock.write {
    _failedStages = Set()
  }

  protected def clearAllJobs(): Unit = lock.write {
    _activeJobs = Set()
    _jobIdToActiveJob = Map()
  }

  private def addStage(jobId: Int, stage: Stage): Unit = {
    _stageIdToStage += (stage.id -> stage)
    updateJobIdStageIdMaps(jobId, stage)
  }

  private def addShuffleStage(
      jobId: Int, stage: ShuffleMapStage, shuffleId: Int): Unit = {

    addStage(jobId, stage)
    _shuffleIdToStageMap += (shuffleId -> stage)

    if (!mapOutputTracker.containsShuffle(shuffleId)) {
      val rdd = stage.rdd
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo(s"Registering RDD ${rdd.id} (${rdd.getCreationSite}) as input to " +
        s"shuffle $shuffleId")
      mapOutputTracker.registerShuffle(shuffleId, rdd.partitions.length)
    }
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]): Unit = {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId

        val stageIds = _jobIdToStageIds.getOrElse(jobId, Set[Int]()) + s.id
        _jobIdToStageIds += jobId -> stageIds

        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }
}
