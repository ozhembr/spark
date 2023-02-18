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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.spark.{ExecutorAllocationClient, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

class ParallelHealthTracker(
   sc: SparkContext,
   client: ExecutorAllocationClient,
   clock: Clock = new SystemClock()) extends HealthTracker {
  private val innerTracker = HealthTracker(sc, Some(client))

  private val delayLatch = new DelayLatch(1.second, clock) // TODO revive delay from config
  private val lock = ThreadUtils.readWriteLock()

  override def applyExcludeOnFailureTimeout(): Unit = {
    if (delayLatch.isAvailable()) {
      lock.write {
        innerTracker.applyExcludeOnFailureTimeout()
      }
    }
  }

  override def isNodeExcluded(node: String): Boolean = lock.read {
    innerTracker.isNodeExcluded(node)
  }

  override def isExecutorExcluded(executorId: String): Boolean = lock.read {
    innerTracker.isExecutorExcluded(executorId)
  }

  override private[scheduler] def killExcludedIdleExecutor(executorId: String): Unit = lock.write {
    innerTracker.killExcludedIdleExecutor(executorId)
  }

  override def handleRemovedExecutor(executorId: String): Unit = lock.write {
    innerTracker.handleRemovedExecutor(executorId)
  }

  override def excludedNodeList(): Set[String] = lock.read {
    innerTracker.excludedNodeList()
  }

  override def updateExcludedForSuccessfulTaskSet(
   stageId: Int, stageAttemptId: Int,
   failuresByExec: mutable.HashMap[String, ExecutorFailuresInTaskSet]): Unit = lock.write {
    innerTracker.updateExcludedForSuccessfulTaskSet(stageId, stageAttemptId, failuresByExec)
  }

  override def updateExcludedForFetchFailure(host: String, executorId: String): Unit = lock.write {
    innerTracker.updateExcludedForFetchFailure(host, executorId)
  }
}

class DelayLatch(delay: FiniteDuration, clock: Clock) {
  private val delayMs = delay.toMillis
  private val atomic = new AtomicLong()

  private def isAvailable(timeMillis: Long): Boolean = {
    atomic.updateAndGet(acc => {
      if (acc + delayMs < timeMillis) {
        timeMillis
      } else {
        acc
      }
    }) == timeMillis
  }

  def isAvailable(): Boolean = {
    isAvailable(clock.getTimeMillis())
  }
}
