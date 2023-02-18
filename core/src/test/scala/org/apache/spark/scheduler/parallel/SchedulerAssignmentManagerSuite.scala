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

import org.mockito.ArgumentMatchers._
import org.mockito.ArgumentMatchers.{eq => argEq}
import org.mockito.Mockito._
import org.scalatest.concurrent.TimeLimits
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkFunSuite, Success, TempLocalSparkContext}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisteredExecutor
import org.apache.spark.util.{CallSite, Clock, SystemClock}

class SchedulerAssignmentManagerSuite
  extends SparkFunSuite with TempLocalSparkContext with TimeLimits {
  private val Executor0 = createReassingExecutor(0)
  private val Executor1 = createReassingExecutor(1)
  private val Executor2 = createReassingExecutor(2)
  private val Executor3 = createReassingExecutor(3)
  private val Executor4 = createReassingExecutor(4)

  implicit val clock: Clock = new SystemClock

  def argExec(expected: RegisteredExecutor*): Seq[RegisteredExecutor] = {
    argThat[Seq[RegisteredExecutor]](_.map(_.executorId) == expected.map(_.executorId))
  }

  def anyOrder[A](expected: Seq[A]): Seq[A] = {
    argThat[Seq[A]](_.toSet == expected.toSet)
  }

  test("SchedulerAssignmentManager should assign jobs according to load") {
    val parallelism = 5

    val mockCallbacks = mock(classOf[ExecutorTracker.Callbacks])
    val executorTracker = new ExecutorTracker(parallelism)
    executorTracker.setCallbacks(mockCallbacks)

    val mockDagSchedulerState = mock(classOf[DAGSchedulerState])

    val mockSchedulerBusyExecutors =
      mock(classOf[Function[Int, Set[String]]])
    val manager =
      new SchedulerAssignmentManager(conf,
        parallelism, mockDagSchedulerState, executorTracker, mockSchedulerBusyExecutors)

    assert(manager.assignTasks(createTaskSet(0, 0, 50)) == 0)
    assert(manager.assignTasks(createTaskSet(1, 1, 40)) == 1)
    assert(manager.assignTasks(createTaskSet(2, 2, 30)) == 2)
    assert(manager.assignTasks(createTaskSet(3, 3, 20)) == 3)
    assert(manager.assignTasks(createTaskSet(4, 4, 10)) == 4)

    assert(manager.assignTasks(createTaskSet(5, 5, 50)) == 4)
    assert(manager.assignTasks(createTaskSet(6, 6, 40)) == 3)
    assert(manager.assignTasks(createTaskSet(7, 7, 30)) == 2)
    assert(manager.assignTasks(createTaskSet(8, 8, 20)) == 1)
    assert(manager.assignTasks(createTaskSet(9, 9, 10)) == 0)

    assert(manager.assignTasks(createTaskSet(10, 10, 50)) == 0)
  }

  test("SchedulerAssignmentManager reassign executors according to load") {
    val parallelism = 5

    val mockCallbacks = mock(classOf[ExecutorTracker.Callbacks])
    val executorTracker = new ExecutorTracker(parallelism)
    executorTracker.setCallbacks(mockCallbacks)

    val mockDagSchedulerState = mock(classOf[DAGSchedulerState])

    val mockSchedulerBusyExecutors =
      mock(classOf[Function[Int, Set[String]]])
    val manager =
      new SchedulerAssignmentManager(conf,
        parallelism, mockDagSchedulerState, executorTracker, mockSchedulerBusyExecutors)

    when(mockDagSchedulerState.stageIdToStage)
      .thenReturn((0 until 5).map(i => i -> createStage(i, i)).toMap)

    assert(manager.assignTasks(createTaskSet(0, 0, 5)) == 0)
    assert(manager.assignTasks(createTaskSet(1, 1, 5)) == 1)
    assert(manager.assignTasks(createTaskSet(2, 2, 5)) == 2)
    assert(manager.assignTasks(createTaskSet(3, 3, 5)) == 3)
    assert(manager.assignTasks(createTaskSet(4, 4, 5)) == 4)

    assert(manager.assignExecutor(Executor0.registerExecutor, Executor0.address).contains(0))
    assert(executorTracker.schedulerExecutorsCount(0) == 1)

    assert(manager.assignExecutor(Executor1.registerExecutor, Executor1.address).contains(1))
    assert(executorTracker.schedulerExecutorsCount(1) == 1)

    assert(manager.assignExecutor(Executor2.registerExecutor, Executor2.address).contains(2))
    assert(executorTracker.schedulerExecutorsCount(2) == 1)

    assert(manager.assignExecutor(Executor3.registerExecutor, Executor3.address).contains(3))
    assert(executorTracker.schedulerExecutorsCount(3) == 1)

    assert(manager.assignExecutor(Executor4.registerExecutor, Executor4.address).contains(4))
    assert(executorTracker.schedulerExecutorsCount(4) == 1)

    // job 0
    // manager.onTaskEnd(createTaskEnd(0, Executor0.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())
    assert(manager.schedulerLoads(0).total == 4)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.asScala == mutable.Map(0 -> 4))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(1, 1, 1, 1, 1))

    // manager.onTaskEnd(createTaskEnd(0, Executor1.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())
    assert(manager.schedulerLoads(0).total == 3)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.asScala == mutable.Map(0 -> 3))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(1, 1, 1, 1, 1))

    // manager.onTaskEnd(createTaskEnd(0, Executor2.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())
    assert(manager.schedulerLoads(0).total == 2)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.asScala == mutable.Map(0 -> 2))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(1, 1, 1, 1, 1))

    // manager.onTaskEnd(createTaskEnd(0, Executor3.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())
    assert(manager.schedulerLoads(0).total == 1)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.asScala == mutable.Map(0 -> 1))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(1, 1, 1, 1, 1))

    // manager.onTaskEnd(createTaskEnd(0, Executor4.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(0), argExec(Executor4), argEq(1))
    assert(manager.schedulerLoads(0).total == 0)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.isEmpty)
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 2, 1, 1, 1))

    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 2, 1, 1, 1)) ==
      Seq(2 -> 4, 3 -> 4, 4 -> 4, 1 -> 3))

    // job 1
    reset(mockCallbacks)

    // manager.onTaskEnd(createTaskEnd(1, Executor0.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(1, Executor1.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(1, Executor2.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(1, Executor3.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(1), argExec(Executor3), argEq(2))

    // manager.onTaskEnd(createTaskEnd(1, Executor4.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(1), argExec(Executor4), argEq(3))

    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 2, 2, 1))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 2, 2, 1)) ==
      Seq(4 -> 4, 2 -> 3, 3 -> 3))

    assert(manager.schedulerLoads(0).total == 0)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.isEmpty)

    assert(manager.schedulerLoads(1).total == 0)
    assert(manager.schedulerLoads(1).stageIdToTaskNum.isEmpty)

    assert(manager.schedulerLoads(2).total == 5)
    assert(manager.schedulerLoads(2).stageIdToTaskNum.asScala == mutable.Map(2 -> 5))

    assert(manager.schedulerLoads(3).total == 5)
    assert(manager.schedulerLoads(3).stageIdToTaskNum.asScala == mutable.Map(3 -> 5))

    assert(manager.schedulerLoads(4).total == 5)
    assert(manager.schedulerLoads(4).stageIdToTaskNum.asScala == mutable.Map(4 -> 5))

    // job 3
    reset(mockCallbacks)

    // manager.onTaskEnd(createTaskEnd(3, Executor0.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(3, Executor1.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(3, Executor2.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(3, Executor3.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(3), argExec(Executor3), argEq(4))

    // manager.onTaskEnd(createTaskEnd(3, Executor4.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(3), argExec(Executor4), argEq(2))

    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 3, 0, 2))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 3, 0, 2)) ==
      Seq(4 -> 3, 2 -> 2))

    assert(manager.schedulerLoads(0).total == 0)
    assert(manager.schedulerLoads(0).stageIdToTaskNum.isEmpty)

    assert(manager.schedulerLoads(1).total == 0)
    assert(manager.schedulerLoads(1).stageIdToTaskNum.isEmpty)

    assert(manager.schedulerLoads(2).total == 5)
    assert(manager.schedulerLoads(2).stageIdToTaskNum.asScala == mutable.Map(2 -> 5))

    assert(manager.schedulerLoads(3).total == 0)
    assert(manager.schedulerLoads(3).stageIdToTaskNum.isEmpty)

    assert(manager.schedulerLoads(4).total == 5)
    assert(manager.schedulerLoads(4).stageIdToTaskNum.asScala == mutable.Map(4 -> 5))

    // job 2
    reset(mockCallbacks)

    // manager.onTaskEnd(createTaskEnd(2, Executor0.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(2, Executor1.executorId))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    // manager.onTaskEnd(createTaskEnd(2, Executor2.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(2), argExec(Executor2), argEq(4))

    // manager.onTaskEnd(createTaskEnd(2, Executor3.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(2), argExec(Executor3), argEq(4))

    // manager.onTaskEnd(createTaskEnd(2, Executor4.executorId))
    verify(mockCallbacks).reassignExecutors(argEq(2), argExec(Executor4), argEq(4))

    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 5))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 5)).isEmpty)

    // job 4
    reset(mockCallbacks)

    // manager.onTaskEnd(createTaskEnd(4, Executor0.executorId))
    verify(mockCallbacks).suspendExecutors(4, Seq(Executor0.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 4))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 4)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0.executorId))

    // manager.onTaskEnd(createTaskEnd(4, Executor1.executorId))
    verify(mockCallbacks).suspendExecutors(4, Seq(Executor1.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 3))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 3)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1).map(_.executorId))

    // manager.onTaskEnd(createTaskEnd(4, Executor2.executorId))
    verify(mockCallbacks).suspendExecutors(4, Seq(Executor2.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 2))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 2)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor2).map(_.executorId))

    // manager.onTaskEnd(createTaskEnd(4, Executor3.executorId))
    verify(mockCallbacks).suspendExecutors(4, Seq(Executor3.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 1))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 1)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor2, Executor3)
      .map(_.executorId))

    // manager.onTaskEnd(createTaskEnd(4, Executor4.executorId))
    verify(mockCallbacks).suspendExecutors(4, Seq(Executor4.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 0)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor2, Executor3, Executor4)
      .map(_.executorId))

    assert(manager.assignTasks(createTaskSet(5, 5, 4)) == 0)
    verify(mockCallbacks)
      .assignExecutors(argEq(0), argExec(Executor0, Executor1, Executor2, Executor3))
    assert(executorTracker.stash == Seq(Executor4.executorId))
  }

  test("SchedulerAssignmentManager should stash all executors if no other jobs were assigned") {
    val parallelism = 5

    val mockDagSchedulerState = mock(classOf[DAGSchedulerState])

    val mockCallbacks = mock(classOf[ExecutorTracker.Callbacks])
    val executorTracker = new ExecutorTracker(parallelism)
    executorTracker.setCallbacks(mockCallbacks)

    val mockSchedulerBusyExecutors =
      mock(classOf[Function[Int, Set[String]]])
    val manager =
      new SchedulerAssignmentManager(conf,
        parallelism, mockDagSchedulerState, executorTracker, mockSchedulerBusyExecutors)

    when(mockDagSchedulerState.stageIdToStage)
      .thenReturn((0 until 5).map(i => i -> createStage(i, i)).toMap)

    assert(manager.assignTasks(createTaskSet(0, 0, 5)) == 0)

    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 0)) == Seq(0 -> 5))
    assert(manager.starvingSchedulers(0, IndexedSeq(0, 0, 0, 0, 0)).isEmpty)

    assert(manager.assignExecutor(Executor0.registerExecutor, Executor0.address).contains(0))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(1, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(1, 0, 0, 0, 0)) ==
      Seq(0 -> 4))

    assert(manager.assignExecutor(Executor1.registerExecutor, Executor1.address).contains(0))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(2, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(2, 0, 0, 0, 0)) ==
      Seq(0 -> 3))

    assert(manager.assignExecutor(Executor2.registerExecutor, Executor2.address).contains(0))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(3, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(3, 0, 0, 0, 0)) ==
      Seq(0 -> 2))

    assert(manager.assignExecutor(Executor3.registerExecutor, Executor3.address).contains(0))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(4, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(4, 0, 0, 0, 0)) ==
      Seq(0 -> 1))

    assert(manager.assignExecutor(Executor4.registerExecutor, Executor4.address).contains(0))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(5, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(5, 0, 0, 0, 0)).isEmpty)

    // manager.onTaskEnd(createTaskEnd(0, Executor0.executorId))
    verify(mockCallbacks).suspendExecutors(0, Seq(Executor0.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(4, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(4, 0, 0, 0, 0)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0.executorId))

    // manager.onTaskEnd(createTaskEnd(0, Executor1.executorId))
    verify(mockCallbacks).suspendExecutors(0, Seq(Executor1.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(3, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(3, 0, 0, 0, 0)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1)
      .map(_.executorId))

    // manager.onTaskEnd(createTaskEnd(0, Executor2.executorId))
    verify(mockCallbacks).suspendExecutors(0, Seq(Executor2.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(2, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(2, 0, 0, 0, 0)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor2)
      .map(_.executorId))

    // manager.onTaskEnd(createTaskEnd(0, Executor3.executorId))
    verify(mockCallbacks).suspendExecutors(0, Seq(Executor3.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(1, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(1, 0, 0, 0, 0)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor2, Executor3)
      .map(_.executorId))

    // manager.onTaskEnd(createTaskEnd(0, Executor4.executorId))
    verify(mockCallbacks).suspendExecutors(0, Seq(Executor4.executorId))
    assert((0 until 5).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0, 0, 0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0, 0, 0, 0)).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor2, Executor3, Executor4)
      .map(_.executorId))

    // all executor -> scheduler mappings were removed

    assert(executorTracker.removeExecutor(Executor2.executorId).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor1, Executor3, Executor4)
      .map(_.executorId))

    assert(executorTracker.removeByAddress(Executor1.address).isEmpty)
    assert(executorTracker.stash == Seq(Executor0, Executor3, Executor4)
      .map(_.executorId))

    assert(executorTracker.removeHost(Executor0.registerExecutor.hostname).isEmpty)
    assert(executorTracker.stash.isEmpty)
  }

  test("SchedulerAssignmentManager should stash or reassign extra executors on job failure") {
    val parallelism = 2

    val mockCallbacks = mock(classOf[ExecutorTracker.Callbacks])
    val executorTracker = new ExecutorTracker(parallelism)
    executorTracker.setCallbacks(mockCallbacks)

    val mockDagSchedulerState = mock(classOf[DAGSchedulerState])

    val mockSchedulerBusyExecutors =
      mock(classOf[Function[Int, Set[String]]])
    val manager =
      new SchedulerAssignmentManager(conf,
        parallelism, mockDagSchedulerState, executorTracker, mockSchedulerBusyExecutors)

    when(mockDagSchedulerState.stageIdToStage)
      .thenReturn((0 until 5).map(i => i -> createStage(i, i)).toMap)

    assert(manager.assignTasks(createTaskSet(0, 0, 5)) == 0)
    assert(manager.assignTasks(createTaskSet(1, 1, 5)) == 1)

    assert(manager.assignExecutor(Executor0.registerExecutor, Executor0.address).contains(0))
    assert(manager.assignExecutor(Executor1.registerExecutor, Executor1.address).contains(1))
    assert(manager.assignExecutor(Executor2.registerExecutor, Executor2.address).contains(0))

    assert(manager.assignTasks(createTaskSet(2, 2, 2)) == 0)

    // manager.onJobEnd(SparkListenerJobEnd(0, 0, JobFailed(new Exception)))
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    assert((0 until 2).map(manager.schedulerLoads(_).total) == Seq(2, 5))
    assert((0 until 2).map(executorTracker.schedulerExecutorsCount) == Seq(2, 1))
    assert(manager.starvingSchedulers(-1, IndexedSeq(2, 1)) == Seq(1 -> 4))

    when(mockSchedulerBusyExecutors.apply(0)).thenReturn(Set[String]())

    // manager.onJobEnd(SparkListenerJobEnd(2, 0, JobFailed(new Exception)))

    verify(mockSchedulerBusyExecutors).apply(0)

    assert((0 until 2).map(manager.schedulerLoads(_).total) == Seq(0, 5))
    assert((0 until 2).map(executorTracker.schedulerExecutorsCount) == Seq(0, 3))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 3)) == Seq(1 -> 2))
    verify(mockCallbacks)
      .reassignExecutors(0, Seq(Executor0, Executor2), 1)
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())

    reset(mockCallbacks)

    when(mockSchedulerBusyExecutors.apply(1)).thenReturn(Set[String]())

    // manager.onJobEnd(SparkListenerJobEnd(1, 0, JobFailed(new Exception)))

    verify(mockSchedulerBusyExecutors).apply(1)

    assert((0 until 2).map(manager.schedulerLoads(_).total) == Seq(0, 0))
    assert((0 until 2).map(executorTracker.schedulerExecutorsCount) == Seq(0, 0))
    assert(manager.starvingSchedulers(-1, IndexedSeq(0, 0)).isEmpty)
    verify(mockCallbacks, never())
      .reassignExecutors(anyInt(), any(), anyInt())
    verify(mockCallbacks)
      .suspendExecutors(argEq(1), anyOrder(Seq(Executor0, Executor1, Executor2).map(_.executorId)))
    assert(executorTracker.stash.size == 3)
    assert(Seq(Executor0, Executor1, Executor2)
      .map(_.executorId)
      .forall(executorTracker.stash.contains))
  }

  test("SchedulerAssignmentManager should ignore busy executors on job finish") {
    val parallelism = 2

    val mockCallbacks = mock(classOf[ExecutorTracker.Callbacks])
    val executorTracker = new ExecutorTracker(parallelism)
    executorTracker.setCallbacks(mockCallbacks)

    val mockDagSchedulerState = mock(classOf[DAGSchedulerState])

    val mockSchedulerBusyExecutors =
      mock(classOf[Function[Int, Set[String]]])
    val manager =
      new SchedulerAssignmentManager(conf,
        parallelism, mockDagSchedulerState, executorTracker, mockSchedulerBusyExecutors)

    when(mockDagSchedulerState.stageIdToStage)
      .thenReturn((0 until 5).map(i => i -> createStage(i, i)).toMap)

    assert(manager.assignTasks(createTaskSet(0, 0, 2)) == 0)
    assert(manager.assignExecutor(Executor0.registerExecutor, Executor0.address).contains(0))
    assert(manager.assignExecutor(Executor1.registerExecutor, Executor1.address).contains(0))

    assert(manager.assignTasks(createTaskSet(1, 1, 5)) == 1)
    assert(manager.assignTasks(createTaskSet(2, 2, 1)) == 0)

    assert((0 until 2).map(manager.schedulerLoads(_).total) == Seq(3, 5))
    assert((0 until 2).map(executorTracker.schedulerExecutorsCount) == Seq(2, 0))

    when(mockSchedulerBusyExecutors.apply(0)).thenReturn(Set[String](Executor0.executorId))

    // manager.onJobEnd(SparkListenerJobEnd(0, 0, JobSucceeded))

    verify(mockSchedulerBusyExecutors).apply(0)

    assert(manager.schedulerLoads(0).total == 1)
    assert((0 until 2).map(executorTracker.schedulerExecutorsCount) == Seq(1, 1))
    verify(mockCallbacks)
      .reassignExecutors(0, Seq(Executor1), 1)
    verify(mockCallbacks, never())
      .suspendExecutors(anyInt(), any())
  }

  private def createTaskSet(jobId: Int, stageId: Int, tasks: Int): TaskSet = {
    new TaskSet(new Array[Task[_]](tasks), stageId, 0, jobId, null, 0)
  }

  private def createStage(jobId: Int, stageId: Int) = {
    val stage = new ResultStage(stageId, sc.emptyRDD, null, Array(), Nil, 0, CallSite.empty, 0)

    stage.jobIds.add(jobId)

    stage
  }

  private def createTaskEnd(stageId: Int, executorId: String): SparkListenerTaskEnd = {
    SparkListenerTaskEnd(stageId, 0, "", Success,
      new TaskInfo(
        0,
        0,
        0,
        0,
        executorId,
        null,
        null,
        false),
      null, null)
  }

  private def createReassingExecutor(id: Int, host: String = "localhost"): RegisteredExecutor = {
    val rer = RER(id, host)
    RegisteredExecutor(
      CoarseGrainedClusterMessages
        .RegisterExecutor(s"ex$id", rer, host, 1, Map(), Map(), Map(), 0),
      rer.address)
  }

  case class RER(id: Int, host: String) extends RpcEndpointRef(conf) {
    override def address: RpcAddress = RpcAddress(host, id)
    override def name: String = s"ex$id"
    override def send(message: Any): Unit = {}
    override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
      Future.successful(null.asInstanceOf[T])
    }
  }
}
