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

package org.apache.spark.scheduler.cluster

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.parallel.{ParallelSchedulerBackend, ParallelTaskScheduler}

/**
 * Cluster Manager for creation of Yarn scheduler and backend
 */
private[spark] class YarnClusterManager extends ExternalClusterManager {

  private def isParallelSchedulers(sc: SparkContext): Boolean = {
    sc.conf
      .getOption("spark.driver.schedulers.parallelism")
      .exists(_.toInt > 1)
  }

  override def canCreate(masterURL: String): Boolean = {
    masterURL == "yarn"
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    sc.deployMode match {
      case "cluster" => new YarnClusterScheduler(sc)
      case "client" =>
        if (isParallelSchedulers(sc)) {
          new YarnSchedulerPar(sc)
        } else {
          new YarnScheduler(sc)
        }
      case _ => throw new SparkException(s"Unknown deploy mode '${sc.deployMode}' for Yarn")
    }
  }

  override def createSchedulerBackend(sparkContext: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    sparkContext.deployMode match {
      case "cluster" =>
        new YarnClusterSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sparkContext)
      case "client" =>
        if (isParallelSchedulers(sparkContext)) {
          new ParallelSchedulerBackend with YarnClientSchedulerBackend {
            override def sc: SparkContext = sparkContext

            override val parallelTaskScheduler: ParallelTaskScheduler =
              scheduler.asInstanceOf[ParallelTaskScheduler]
          }
        } else {
          new CoarseGrainedSchedulerBackend with YarnClientSchedulerBackend {
            override def sc: SparkContext = sparkContext
            override def taskScheduler: TaskScheduler = scheduler
            override def id: Option[Int] = None
          }
        }
      case  _ =>
        throw new SparkException(s"Unknown deploy mode '${sparkContext.deployMode}' for Yarn")
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.initialize(backend)
  }
}
