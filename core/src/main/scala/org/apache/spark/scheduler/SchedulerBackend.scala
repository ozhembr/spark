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

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.storage.BlockManagerId

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
private[spark] trait SchedulerBackend extends Logging {
  private val appId = "spark-application-" + System.currentTimeMillis

  def sc: SparkContext
  lazy val rpcEnv: RpcEnv = sc.env.rpcEnv
  lazy val conf: SparkConf = sc.conf
  lazy val listenerBus: LiveListenerBus = sc.listenerBus

  // Spark configuration sent to executors. This is a lazy val so that subclasses of the
  // scheduler can modify the SparkConf object before this view is created.
  lazy val sparkProperties: Seq[(String, String)] = conf.getAll
    .filter { case (k, _) => k.startsWith("spark.") }
    .toSeq

  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  def minRegisteredRatio: Double =
    math.min(1, conf.get(config.SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).getOrElse(0.0))

  val driverEndpoint: RpcEndpointRef

  // The token manager used to create security tokens.
  protected var delegationTokenManager: Option[HadoopDelegationTokenManager] = None

  // Current set of delegation tokens to send to executors.
  protected val delegationTokens = new AtomicReference[Array[Byte]]()

  def taskScheduler: TaskScheduler

  def reset(): Unit = {}

  /**
   * Create the delegation token manager to be used for the application. This method is called
   * once during the start of the scheduler backend (so after the object has already been
   * fully constructed), only if security is enabled in the Hadoop configuration.
   */
  protected def createTokenManager(): Option[HadoopDelegationTokenManager] = None

  def currentDelegationTokens: Array[Byte] = delegationTokens.get()

  def updateDelegationTokens(tokens: Array[Byte]): Unit = {}

  def start(): Unit
  def stop(): Unit
  /**
   * Update the current offers and schedule tasks
   */
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  /**
   * Requests that an executor kills a running task.
   *
   * @param taskId Id of the task.
   * @param executorId Id of the executor the task is running on.
   * @param interruptThread Whether the executor should interrupt the task thread.
   * @param reason The reason for the task kill.
   */
  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException

  def isReady(): Boolean = true

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

  /**
   * Get the attributes on driver. These attributes are used to replace log URLs when
   * custom log url pattern is specified.
   * @return Map containing attributes on driver.
   */
  def getDriverAttributes: Option[Map[String, String]] = None

  /**
   * Get the max number of tasks that can be concurrent launched based on the ResourceProfile
   * could be used, even if some of them are being used at the moment.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @param rp ResourceProfile which to use to calculate max concurrent tasks.
   * @return The max number of tasks that can be concurrent launched currently.
   */
  def maxNumConcurrentTasks(rp: ResourceProfile): Int = 0

  /**
   * Get the list of host locations for push based shuffle
   *
   * Currently push based shuffle is disabled for both stage retry and stage reuse cases
   * (for eg: in the case where few partitions are lost due to failure). Hence this method
   * should be invoked only once for a ShuffleDependency.
   * @return List of external shuffle services locations
   */
  def getShufflePushMergerLocations(
      numPartitions: Int,
      resourceProfileId: Int): Seq[BlockManagerId] = Nil

  def requestTotalExecutors(
      resourceProfileIdToNumExecutors: Map[Int, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]): Boolean = false

  protected def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]): Future[Boolean] =
    Future.successful(false)

  private[scheduler] def disableExecutor(executorId: String): Boolean = false

  protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)

  def excludedNodes(): Set[String] = taskScheduler.excludedNodes()

  def getTotalRegisteredExecutors(): Int = 0

  def lastAllocatedExecutorId: Int = 0

  def sufficientResourcesRegistered(): Boolean = false

  def onDisconnectedExecutor(rpcAddress: RpcAddress, executorId: String): Unit = {}
}
