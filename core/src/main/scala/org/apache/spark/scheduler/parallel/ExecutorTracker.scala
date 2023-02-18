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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.ThreadUtils

class ExecutorTracker(parallelism: Int) extends Logging {
  private val _schedulersExecutorCount: IndexedSeq[AtomicInteger] =
    IndexedSeq.fill(parallelism)(new AtomicInteger())

  private val lock = ThreadUtils.readWriteLock()

  private val executorIdToSchedulerIdMap = new ConcurrentHashMap[String, Int]().asScala
  private val executorIdToExecInfoMap = mutable.Map[String, RegisteredExecutor]()
  private val executorIdToHost = mutable.Map[String, String]()
  private val hostToExecutorIds = mutable.Map[String, Set[String]]()
  private val addressToExecutorId = mutable.Map[RpcAddress, String]()

  private var callbacks: ExecutorTracker.Callbacks = _

  @volatile private[parallel] var stash = Vector[String]() // executorId

  def getStash: Vector[String] = stash

  private def removeAddressToExecutorId(executorId: String): Unit = {
    addressToExecutorId
      .find(_._2 == executorId)
      .foreach {
        case (address, _) =>
          addressToExecutorId.remove(address)
      }
  }

  private def printAmount(): Unit = {
    logDebug(
      s"Active executors=${executorIdToSchedulerIdMap.size} hosts=${hostToExecutorIds.size}" +
      s" counters=${_schedulersExecutorCount.mkString(",")} stashed=${stash.length}")
  }

  def setCallbacks(callbacks: ExecutorTracker.Callbacks): Unit = {
    this.callbacks = callbacks
  }

  def schedulerExecutorsCount(schedulerId: Int): Int = {
    _schedulersExecutorCount(schedulerId).get()
  }

  private def schedulerExecutors(schedulerId: Int): Seq[String] = {
    var seq = Vector[String]()

    executorIdToSchedulerIdMap
      .foreach {
        case (executorId, schedId) =>
          if (schedId == schedulerId) {
            seq +:= executorId
          }
      }

    seq
  }

  def addExecutor(
    registerExecutor: RegisterExecutor,
    callerAddr: RpcAddress,
    starvingScheduler: IndexedSeq[Int] => Option[Int]): Option[Int] = lock.write {
    val schedulerIdOpt = starvingScheduler(_schedulersExecutorCount.map(_.get()))
    val executorId = registerExecutor.executorId
    val host = registerExecutor.hostname
    val address =
      if (registerExecutor.executorRef.address != null) {
        registerExecutor.executorRef.address
      } else {
        callerAddr
      }

    logDebug(s"addExecutor executorId=$executorId host=$host address=$address" +
      s" to schedulerIdOpt=$schedulerIdOpt")

    executorIdToHost.put(executorId, host)
    hostToExecutorIds.put(
      host,
      hostToExecutorIds.getOrElse(host, Set()) + executorId
    )
    addressToExecutorId.put(address, executorId)

    executorIdToExecInfoMap
      .update(
        registerExecutor.executorId,
        RegisteredExecutor(registerExecutor, address))

    printAmount()

    schedulerIdOpt match {
      case Some(schedulerId) =>
        _schedulersExecutorCount(schedulerId).incrementAndGet()

        executorIdToSchedulerIdMap.put(executorId, schedulerId)
          .foreach { prevSchedulerId =>
            logDebug(s"Executor id=$executorId already existed for schedulerId=$prevSchedulerId" +
              s" nextSchedulerId=$schedulerId")
          }

        Some(schedulerId)

      case _ =>
        stash :+= executorId
        None
    }
  }

  private def reassignExecutors(
    fromSchedulerId: Int,
    executors: Seq[String],
    toSchedulerId: Int): Seq[RegisteredExecutor] = {
    logDebug(s"reassignExecutors($fromSchedulerId, $executors, $toSchedulerId)" +
      s" counters=${_schedulersExecutorCount.mkString(",")}" +
      s" executorIdToSchedulerIdMap=" +
      s"${executorIdToSchedulerIdMap.toSeq.sortBy(_._2).mkString(",")}" +
      s" executorIdToRegisterInfoMap=${executorIdToExecInfoMap.keys.toSeq.sorted.mkString(",")}")

    var registeredExecutors = Vector[RegisteredExecutor]()

    executors
      .foreach { executorId =>
        executorIdToSchedulerIdMap
          .put(executorId, toSchedulerId)
          .foreach { prevSchedulerId =>
            if (prevSchedulerId == fromSchedulerId) {
              _schedulersExecutorCount(fromSchedulerId).decrementAndGet()
              _schedulersExecutorCount(toSchedulerId).incrementAndGet()

              registeredExecutors :+= executorIdToExecInfoMap(executorId)
            } else {
              logDebug(s"Executor id=$executorId was assigned to " +
                s"schedulerId=$prevSchedulerId instead of expected schedulerId=$fromSchedulerId")
            }
          }
      }

    printAmount()

    registeredExecutors
  }

  private def stashExecutors(schedulerId: Int, executorIds: Seq[String]): Unit = {
    logDebug(s"stashExecutors($schedulerId, $executorIds)")
    stash ++= executorIds

    executorIds
      .foreach { executorId =>
        if (executorIdToSchedulerIdMap.remove(executorId).nonEmpty) {
          _schedulersExecutorCount(schedulerId).decrementAndGet()
        } else {
          logDebug(s"Executor id=$executorId is missing," +
            s" but expected to be assigned for schedulerId=$schedulerId")
        }
      }
  }

  def extra(schedulerId: Int,
            getSchedulerLoad: => Int,
            getBusyExecutors: => Set[String],
            getStarvingSchedulers: IndexedSeq[Int] => Seq[(Int, Int)])
           (getCandidates: (Int, Set[String]) => Seq[String]): Unit = lock.write {
      val schedulerLoad = getSchedulerLoad
      val extra = _schedulersExecutorCount(schedulerId).get() - schedulerLoad

      if (extra > 0) {
        val busyExecutors = getBusyExecutors
        logDebug(s"extra schedulerId=$schedulerId busyExecutors=$busyExecutors")

        var idleExecutors = getCandidates(extra, busyExecutors)

        if (idleExecutors.nonEmpty) {
          var starvingSchedulers = getStarvingSchedulers(_schedulersExecutorCount.map(_.get()))

          while (idleExecutors.nonEmpty && starvingSchedulers.nonEmpty) {
            val (starvingSchedulerId, starveNum) = starvingSchedulers.head
            val (executorIds, restOfExecutors) = idleExecutors.splitAt(starveNum)

            reassignExecutors(
              schedulerId,
              callbacks.reassignExecutors(
                schedulerId,
                executorIds.map(executorIdToExecInfoMap(_)),
                starvingSchedulerId),
              starvingSchedulerId)

            starvingSchedulers = starvingSchedulers.tail
            idleExecutors = restOfExecutors
          }

          if (idleExecutors.nonEmpty) {
            stashExecutors(
              schedulerId,
              callbacks.suspendExecutors(schedulerId, idleExecutors))
          }
        }
      }
    }

  def extraExecutors(schedulerId: Int,
                     candidates: Seq[String],
                     getSchedulerLoad: => Int,
                     getBusyExecutors: => Set[String],
                     getStarvingSchedulers: IndexedSeq[Int] => Seq[(Int, Int)]): Unit = {
    extra(schedulerId, getSchedulerLoad, getBusyExecutors, getStarvingSchedulers) {
      (extraCount, busyExecutors) =>
        val idleExecutors = candidates.filterNot(busyExecutors(_))

        if (idleExecutors.length > extraCount) {
          logError(s"Wrong count for extra executors for schedulerId=$schedulerId" +
            s" extra=$extraCount idle=${idleExecutors.length}")
        }

        idleExecutors
    }
  }

  def extraExecutors(schedulerId: Int,
                     getSchedulerLoad: => Int,
                     getBusyExecutors: => Set[String],
                     getStarvingSchedulers: IndexedSeq[Int] => Seq[(Int, Int)]): Unit = {
    extra(schedulerId, getSchedulerLoad, getBusyExecutors, getStarvingSchedulers) {
      (extraCount, busyExecutors) =>
        var idleExecutors = Vector[String]()

        schedulerExecutors(schedulerId)
          .takeWhile { executorId =>
            if (!busyExecutors(executorId)) {
              idleExecutors :+= executorId
            }

            extraCount > idleExecutors.length
          }

        idleExecutors
    }
  }

  def unstashExecutor(schedulerId: Int, n: Int): Unit = {
    val executors = lock.write {
      if (stash.nonEmpty) {
        logDebug(s"unstashExecutor($schedulerId, $n)")

        var i = 0
        var registeredExecutors = Vector[RegisteredExecutor]()

        stash.takeWhile { executorId =>
          if (executorIdToSchedulerIdMap.put(executorId, schedulerId).isEmpty) {
            _schedulersExecutorCount(schedulerId).incrementAndGet()
            registeredExecutors :+= executorIdToExecInfoMap(executorId)
          } else {
            logError(
              s"Executor id=$executorId unexpected assignment to schedulerId=$schedulerId")
          }

          i += 1

          registeredExecutors.length < n
        }

        stash = stash.drop(i)

        registeredExecutors
      } else {
        Seq()
      }
    }

    if (executors.nonEmpty) {
      callbacks.assignExecutors(schedulerId, executors)
    }
  }

  def groupExecutorsByScheduler(executorIds: Seq[String]): Seq[(Seq[String], Int)] = lock.read {
    val schedulerExecutors = Array.fill(parallelism)(Vector[String]())

    executorIds
      .foreach { executorId =>
        executorIdToSchedulerIdMap
          .get(executorId)
          .foreach { schedulerId =>
            val vector = schedulerExecutors(schedulerId)
            schedulerExecutors(schedulerId) = vector :+ executorId
          }
      }

    val result = schedulerExecutors.zipWithIndex.toSeq.filter(_._1.nonEmpty)

    logDebug(s"groupExecutorsByScheduler $executorIds\n$result")

    result
  }

  def indexByExecutorId(executorId: String): Option[Int] = {
    executorIdToSchedulerIdMap.get(executorId)
  }

  def indexesByHost(host: String): Set[Int] = lock.read {
    hostToExecutorIds.getOrElse(host, Set[String]())
      .map(executorIdToSchedulerIdMap(_))
  }

  def executorIdByAddress(address: RpcAddress): Option[String] = lock.read {
    addressToExecutorId.get(address)
  }

  def executorIds: Seq[String] = {
    executorIdToSchedulerIdMap.keys.toSeq
  }

  def totalExecutors: Int = {
    executorIdToSchedulerIdMap.size
  }

  def removeHost(host: String): Set[Int] = lock.write {
    val set =
      hostToExecutorIds
        .remove(host)
        .getOrElse(Set())
        .flatMap { executorId =>
          executorIdToHost.remove(executorId)

          removeAddressToExecutorId(executorId)

          executorIdToExecInfoMap.remove(executorId)
          stash = stash.filter(_ != executorId)
          val schedulerIdOpt =
            executorIdToSchedulerIdMap
              .remove(executorId)
          schedulerIdOpt.foreach(_schedulersExecutorCount(_).decrementAndGet())

          schedulerIdOpt
        }

    logDebug(s"removeHost $host $set")

    printAmount()

    set
  }

  private def removeExecutorInner(executorId: String): Option[Int] = {
    executorIdToExecInfoMap.remove(executorId)

    val index = executorIdToSchedulerIdMap.remove(executorId)

    logDebug(s"removeExecutor $executorId $index")

    index match {
      case Some(i) =>
        _schedulersExecutorCount(i).decrementAndGet()
      case _ =>
        stash = stash.filter(_ != executorId)
    }

    executorIdToHost
      .remove(executorId)
      .foreach { host =>
        val remaining = hostToExecutorIds(host) - executorId

        if (remaining.nonEmpty) {
          hostToExecutorIds.update(host, remaining)
        } else {
          hostToExecutorIds.remove(host)
        }
      }

    printAmount()

    index
  }

  def removeExecutor(executorId: String): Option[Int] = lock.write {
    removeAddressToExecutorId(executorId)
    removeExecutorInner(executorId)
  }

  def removeByAddress(address: RpcAddress): Option[(String, Int)] = lock.write {
    val result =
      for {
        executorId <- addressToExecutorId.remove(address)
        index <- removeExecutorInner(executorId)
      } yield executorId -> index

    logDebug(s"removeByAddress $address $result")

    printAmount()

    result
  }
}

object ExecutorTracker {
  trait Callbacks {
    def reassignExecutors(
      oldSchedulerId: Int, seq: Seq[RegisteredExecutor], nextSchedulerId: Int): Seq[String]
    def suspendExecutors(schedulerId: Int, executorIds: Seq[String]): Seq[String]
    def assignExecutors(schedulerId: Int, seq: Seq[RegisteredExecutor]): Unit
  }
}
