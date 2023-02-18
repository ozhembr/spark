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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.Future

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.ExecutorLogUrlHandler
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

/**
 * A scheduler backend that waits for coarse-grained executors to connect.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
trait CoarseGrainedSchedulerBackend extends ExecutorAllocationClient
  with SchedulerBackend with SchedulerBackendCommon {
  def id: Option[Int]

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  protected val totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  protected val totalRegisteredExecutors = new AtomicInteger(0)
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  private val lock = ThreadUtils.readWriteLock()

  private val offersRef = new AtomicReference[List[String]](Nil)

  // Accessing `executorDataMap` in the inherited methods from ThreadSafeRpcEndpoint doesn't need
  // any protection. But accessing `executorDataMap` out of the inherited methods must be
  // protected by `CoarseGrainedSchedulerBackend.this`. Besides, `executorDataMap` should only
  // be modified in the inherited methods from ThreadSafeRpcEndpoint with protection by
  // `CoarseGrainedSchedulerBackend.this`.
  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors for each ResourceProfile requested by the cluster
  // manager, [[ExecutorAllocationManager]]
  @GuardedBy("lock")
  private val requestedTotalExecutorsPerResourceProfile = new HashMap[ResourceProfile, Int]

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure). Visible for testing only.
  @GuardedBy("lock")
  private[scheduler] val executorsPendingToRemove = new HashMap[String, Boolean]

  // Executors that have been lost, but for which we don't yet know the real exit reason.
  private val executorsPendingLossReason = new HashSet[String]

  // Executors which are being decommissioned. Maps from executorId to workerHost.
  protected val executorsPendingDecommission = new HashMap[String, Option[String]]

  // A map of ResourceProfile id to map of hostname with its possible task number running on it
  @GuardedBy("lock")
  protected var rpHostToLocalTaskCount: Map[Int, Map[String, Int]] = Map.empty

  // The number of pending tasks per ResourceProfile id which is locality required
  @GuardedBy("lock")
  protected var numLocalityAwareTasksPerResourceProfileId = Map.empty[Int, Int]

  // The num of current max ExecutorId used to re-register appMaster
  @volatile protected var currentExecutorIdCounter = 0

  private val reviveThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "driver-revive-thread" + id.map(i => s"-$i").getOrElse(""))

  private val offerThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "driver-offer-thread" + id.map(i => s"-$i").getOrElse("")
    )

  class DriverEndpoint extends IsolatedRpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = CoarseGrainedSchedulerBackend.this.rpcEnv

    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    private val logUrlHandler: ExecutorLogUrlHandler = new ExecutorLogUrlHandler(
      conf.get(UI.CUSTOM_EXECUTOR_LOG_URL))

    private lazy val emptyRpcCallContext = new RpcCallContext {
      override def reply(response: Any): Unit = {}
      override def sendFailure(e: Throwable): Unit = {
        logError("emptyRpcCallContext", e)
      }
      override def senderAddress: RpcAddress = DriverEndpoint.this.self.address
    }

    override def onStart(): Unit = {
      // Periodically revive offers to allow delay scheduling to work
      val reviveIntervalMs = conf.get(SCHEDULER_REVIVE_INTERVAL).getOrElse(1000L)

      reviveThread.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(ReviveOffers))
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)

      val offerIntervalMs =
        conf.getOption("spark.scheduler.offer.interval")
          .map(_.toLong)
          .getOrElse((reviveIntervalMs / 5).max(50))

      var lastTime = 0L

      offerThread.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
        val time = System.currentTimeMillis()

        if (lastTime + offerIntervalMs < time) {
          lastTime = time
          makePendingOffers()
        }
      }, offerIntervalMs, offerIntervalMs, TimeUnit.MILLISECONDS)
    }

    def registerExecutor(context: RpcCallContext,
                         regExecutor: RegisterExecutor,
                         executorAddress: RpcAddress): Unit = {
      val RegisterExecutor(executorId, executorRef, hostname, cores, logUrls,
        attributes, resources, resourceProfileId) = regExecutor

      if (executorDataMap.contains(executorId)) {
        context.sendFailure(new IllegalStateException(s"Duplicate executor ID: $executorId"))
      } else if (taskScheduler.excludedNodes.contains(hostname) ||
        isExecutorExcluded(executorId, hostname)) {
        // If the cluster manager gives us an executor on an excluded node (because it
        // already started allocating those resources before we informed it of our exclusion,
        // or if it ignored our exclusion), then we reject that executor immediately.
        logInfo(s"Rejecting $executorId as it has been excluded.")
        context.sendFailure(
          new IllegalStateException(s"Executor is excluded due to failures: $executorId"))
      } else {
        logError(s"Registered executor $executorRef ($executorAddress) with ID $executorId, " +
          s" cores=$cores resources=$resources" +
          s" ResourceProfileId $resourceProfileId")
        addressToExecutorId(executorAddress) = executorId
        totalCoreCount.addAndGet(cores)
        totalRegisteredExecutors.addAndGet(1)
        val resourcesInfo = resources.map { case (rName, info) =>
          // tell the executor it can schedule resources up to numSlotsPerAddress times,
          // as configured by the user, or set to 1 as that is the default (1 task/resource)
          val numParts = sc.resourceProfileManager
            .resourceProfileFromId(resourceProfileId).getNumSlotsPerAddress(rName, conf)
          (info.name, new ExecutorResourceInfo(info.name, info.addresses, numParts))
        }
        val data = new ExecutorData(executorRef, executorAddress, hostname,
          0, cores, logUrlHandler.applyPattern(logUrls, attributes), attributes,
          resourcesInfo, resourceProfileId, registrationTs = System.currentTimeMillis())
        // This must be synchronized because variables mutated
        // in this block are read when requesting executors
        lock.write {
          executorDataMap.put(executorId, data)
          if (currentExecutorIdCounter < executorId.toInt) {
            currentExecutorIdCounter = executorId.toInt
          }
        }
        if (id.isEmpty) {
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
        }
        // Note: some tests expect the reply to come after we put the executor in the map
        context.reply(true)
      }
    }

    private def reRegisterExecutor(registeredExecutor: RegisteredExecutor): Unit = {
      val executorId = registeredExecutor.executorId
      val address = registeredExecutor.address

      logInfo(s"Reset executor $executorId $address")

      registerExecutor(emptyRpcCallContext, registeredExecutor.registerExecutor, address)

      val data = executorDataMap(executorId)
      data.setFreeCores(data.totalCores)

      addOffer(executorId)
    }

    override def receive: PartialFunction[Any, Unit] = {
      case registeredExecutor: RegisteredExecutor =>
        reRegisterExecutor(registeredExecutor)

      case RegisteredExecutors(seq) =>
        seq.foreach(reRegisterExecutor)

      case StatusUpdate(executorId, taskId, state, data, resources) => // outer
        taskScheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              val rpId = executorInfo.resourceProfileId
              val prof = sc.resourceProfileManager.resourceProfileFromId(rpId)
              val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(prof, conf)
              executorInfo.addFreeCores(taskCpus)
              resources.foreach { case (k, v) =>
                executorInfo.resourcesInfo
                  .get(k)
                  .foreach(_.release(v.addresses))
              }
              addOffer(executorId)
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }

      case ReviveOffers => // inner
        makeOffers()

      case KillTask(taskId, executorId, interruptThread, reason) => // inner
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(
              KillTask(taskId, executorId, interruptThread, reason))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }

      case KillExecutorsOnHost(host) => // inner
        taskScheduler.getExecutorsAliveOnHost(host).foreach { exec =>
          killExecutors(exec.toSeq, adjustTargetNumExecutors = false, countFailures = false,
            force = true)
        }

      case UpdateDelegationTokens(newDelegationTokens) =>
        updateDelegationTokens(newDelegationTokens)

      case re @ RemoveExecutor(executorId, reason) =>
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
        removeExecutor(executorId, reason)

      case RemoveWorker(workerId, host, message) =>
        removeWorker(workerId, host, message)

      case LaunchedExecutor(executorId) =>
        executorDataMap.get(executorId).foreach { data =>
          data.setFreeCores(data.totalCores)
        }
        addOffer(executorId)
      case e =>
        logError(s"Received unexpected message. ${e}")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case re: RegisterExecutor =>
        // If the executor's rpc env is not listening for incoming connections, `hostPort`
        // will be null, and the client connection should be used to contact the executor.
        val executorAddress = if (re.executorRef.address != null) {
          re.executorRef.address
        } else {
          context.senderAddress
        }
        registerExecutor(context, re, executorAddress)

      case StopDriver =>
        context.reply(true)
        stop()

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)

      case RemoveWorker(workerId, host, message) =>
        removeWorker(workerId, host, message)
        context.reply(true)

      // Do not change this code without running the K8s integration suites
      case ExecutorDecommissioning(executorId) =>
        logWarning(s"Received executor $executorId decommissioned message")
        context.reply(
          decommissionExecutor(
            executorId,
            ExecutorDecommissionInfo(s"Executor $executorId is decommissioned."),
            adjustTargetNumExecutors = false,
            triggeredByExecutor = true))

      case RetrieveSparkAppConfig(resourceProfileId) =>
        val rp = sc.resourceProfileManager.resourceProfileFromId(resourceProfileId)
        val reply = SparkAppConfig(
          sparkProperties,
          SparkEnv.get.securityManager.getIOEncryptionKey(),
          Option(delegationTokens.get()),
          rp)
        context.reply(reply)

      case ise @ IsExecutorAlive(executorId) =>
        context.reply(isExecutorAliveInner(executorId))

      case e =>
        logError(s"Received unexpected ask ${e}")
    }

    // Make fake resource offers on all executors
    private def makeOffers(): Unit = {
      // Make sure no executor is killed while some task is launching on it
      val taskDescs = withLock {
        // Filter out executors under killing
        val workOffers = executorDataMap.collect {
          case (id, executorData) if !isRemoving(id) =>
            new WorkerOffer(id, executorData.executorHost, executorData.getFreeCores,
              Some(executorData.executorAddress.hostPort),
              executorData.resourcesInfo.map { case (rName, rInfo) =>
                (rName, rInfo.availableAddrs.toBuffer)
              }, executorData.resourceProfileId)
        }.toIndexedSeq
        taskScheduler.resourceOffers(workOffers, true)
      }

      if (taskDescs.nonEmpty) {
        launchTasks(taskDescs)
      }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId
        .get(remoteAddress)
        .foreach(onDisconnectedExecutor(remoteAddress, _))
    }

    private def addOffer(executorId: String): List[String] = {
      offersRef.updateAndGet(executorId :: _)
    }

    private def makePendingOffers(): Unit = {
      if (offersRef.get().nonEmpty) {
        // Make sure no executor is killed while some task is launching on it
        withLock {
          val execIds = offersRef.getAndSet(Nil).toSet.toIndexedSeq
          var workOffers = IndexedSeq[WorkerOffer]()

          execIds.foreach {
            executorId =>
              // Filter out executors under killing
              executorDataMap.get(executorId) match {
                case Some(executorData) if isExecutorAliveInner(executorId) =>
                  workOffers :+=
                    WorkerOffer(
                      executorId,
                      executorData.executorHost,
                      executorData.getFreeCores,
                      Some(executorData.executorAddress.hostPort),
                      executorData.resourcesInfo.map {
                        case (rName, rInfo) =>
                          rName -> rInfo.availableAddrs.toBuffer
                      },
                      executorData.resourceProfileId)
                case _ =>
              }
          }

          val taskDescs = taskScheduler.resourceOffers(workOffers, true)

          if (taskDescs.nonEmpty) {
            launchTasks(taskDescs)
          }
        }
      }
    }

    // Launch tasks returned by a set of resource offers
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit = {
      for (task <- tasks.flatten) {
        val serializedTask = TaskDescription.encode(task)
        if (serializedTask.limit() >= maxRpcMessageSize) {
          taskScheduler.taskSetManagerByTaskId(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                s"${RPC_MESSAGE_MAX_SIZE.key} (%d bytes). Consider increasing " +
                s"${RPC_MESSAGE_MAX_SIZE.key} or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit(), maxRpcMessageSize)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        } else {
          val executorData = executorDataMap(task.executorId)
          // Do resources allocation here. The allocated resources will get released after the task
          // finishes.
          val rpId = executorData.resourceProfileId
          val prof = sc.resourceProfileManager.resourceProfileFromId(rpId)
          val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(prof, conf)
          executorData.addFreeCores(-taskCpus)
          task.resources.foreach { case (rName, rInfo) =>
            assert(executorData.resourcesInfo.contains(rName))
            executorData.resourcesInfo(rName).acquire(rInfo.addresses)
          }

          logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
            s"${executorData.executorHost}.")

          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    def clearExecutors(executors: Seq[String]): Seq[String] = {
      withLock {
        val clearExecutors = taskScheduler.clearExecutors(executors)

        logDebug(s"clearExecutors $clearExecutors")

        clearExecutors.foreach { executorId =>
          executorDataMap.remove(executorId).foreach { executorInfo =>
            logDebug(s"clear executorId=$executorId")
            addressToExecutorId -= executorInfo.executorAddress
            executorsPendingLossReason -= executorId
            executorsPendingToRemove.remove(executorId)
            executorsPendingDecommission.remove(executorId)
            totalCoreCount.addAndGet(-executorInfo.totalCores)
            totalRegisteredExecutors.addAndGet(-1)
          }
        }

        clearExecutors
      }
    }

    // Remove a disconnected executor from the cluster
    private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      logDebug(s"Asked to remove executor $executorId with reason $reason")
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          logWarning(s"Some Asked to remove executor $executorId with reason $reason")

          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          val lossReason = lock.write {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingLossReason -= executorId
            val killedByDriver = executorsPendingToRemove.remove(executorId).getOrElse(false)
            val workerHostOpt = executorsPendingDecommission.remove(executorId)
            if (killedByDriver) {
              ExecutorKilled
            } else if (workerHostOpt.isDefined) {
              ExecutorDecommission(workerHostOpt.get)
            } else {
              reason
            }
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          taskScheduler.executorLost(executorId, lossReason)
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
        case None =>
          // SPARK-15262: If an executor is still alive even after the scheduler has removed
          // its metadata, we may receive a heartbeat from that executor and tell its block
          // manager to reregister itself. If that happens, the block manager master will know
          // about the executor, but the scheduler will not. Therefore, we should remove the
          // executor from the block manager when we hit this case.
          sc.env.blockManager.master.removeExecutorAsync(executorId)
          logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    // Remove a lost worker from the cluster
    private def removeWorker(workerId: String, host: String, message: String): Unit = {
      logDebug(s"Asked to remove worker $workerId with reason $message")
      taskScheduler.workerRemoved(workerId, host, message)
    }
  }

  /**
   * Stop making resource offers for the given executor. The executor is marked as lost with
   * the loss reason still pending.
   *
   * @return Whether executor should be disabled
   */
  override private[scheduler] def disableExecutor(executorId: String): Boolean = {
    val shouldDisable = lock.write {
      if (isExecutorAliveInner(executorId)) {
        executorsPendingLossReason += executorId
        true
      } else {
        // Returns true for explicitly killed executors, we also need to get pending loss reasons;
        // For others return false.
        executorsPendingToRemove.contains(executorId)
      }
    }

    if (shouldDisable) {
      taskScheduler.executorLost(executorId, LossReasonPending)
    }

    shouldDisable
  }

  val isolatedRpcEndpoint: DriverEndpoint = createDriverEndpoint()
  override val driverEndpoint: RpcEndpointRef = rpcEnv.setupEndpoint(
    CoarseGrainedSchedulerBackend.endpointName(id), isolatedRpcEndpoint)

  /**
   * Request that the cluster manager decommission the specified executors.
   *
   * @param executorsAndDecomInfo Identifiers of executors & decommission info.
   * @param adjustTargetNumExecutors whether the target number of executors will be adjusted down
   *                                 after these executors have been decommissioned.
   * @param triggeredByExecutor whether the decommission is triggered at executor.
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  override def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = withLock {
    // Do not change this code without running the K8s integration suites
    val executorsToDecommission = executorsAndDecomInfo.flatMap { case (executorId, decomInfo) =>
      // Only bother decommissioning executors which are alive.
      if (isExecutorAliveInner(executorId)) {
        taskScheduler.executorDecommission(executorId, decomInfo)
        executorsPendingDecommission(executorId) = decomInfo.workerHost
        Some(executorId)
      } else {
        None
      }
    }
    logInfo(s"Decommission executors: ${executorsToDecommission.mkString(", ")}")

    // If we don't want to replace the executors we are decommissioning
    if (adjustTargetNumExecutors) {
      adjustExecutors(executorsToDecommission)
    }

    // Mark those corresponding BlockManagers as decommissioned first before we sending
    // decommission notification to executors. So, it's less likely to lead to the race
    // condition where `getPeer` request from the decommissioned executor comes first
    // before the BlockManagers are marked as decommissioned.
    // Note that marking BlockManager as decommissioned doesn't need depend on
    // `spark.storage.decommission.enabled`. Because it's meaningless to save more blocks
    // for the BlockManager since the executor will be shutdown soon.
    sc.env.blockManager.master.decommissionBlockManagers(executorsToDecommission)

    if (!triggeredByExecutor) {
      executorsToDecommission.foreach { executorId =>
        logInfo(s"Notify executor $executorId to decommissioning.")
        executorDataMap(executorId).executorEndpoint.send(DecommissionExecutor)
      }
    }

    executorsToDecommission
  }

  protected def createDriverEndpoint(): DriverEndpoint = new DriverEndpoint()

  def stopExecutors(): Unit = {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askSync[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop(): Unit = {
    reviveThread.shutdownNow()
    offerThread.shutdownNow()
    stopExecutors()
    delegationTokenManager.foreach(_.stop())
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askSync[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure.
   * Visible for testing only.
   * */
  override def reset(): Unit = {
    val executors: Set[String] = lock.write {
      requestedTotalExecutorsPerResourceProfile.clear()
      executorDataMap.keys.toSet
    }

    // Remove all the lingering executors that should be removed but not yet. The reason might be
    // because (1) disconnected event is not yet received; (2) executors die silently.
    executors.foreach { eid =>
      removeExecutor(eid,
        ExecutorProcessLost("Stale executor after cluster manager re-registered."))
    }
  }

  override def reviveOffers(): Unit = Utils.tryLogNonFatalError {
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit = {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread, reason))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  /**
   * Called by subclasses when notified of a lost worker. It just fires the message and returns
   * at once.
   */
  protected def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    driverEndpoint.send(RemoveExecutor(executorId, reason))
  }

  protected def removeWorker(workerId: String, host: String, message: String): Unit = {
    driverEndpoint.send(RemoveWorker(workerId, host, message))
  }

  override def sufficientResourcesRegistered(): Boolean = true

  /**
   * Return the number of executors currently registered with this backend.
   */
  private def numExistingExecutors: Int = lock.read { executorDataMap.size }

  override def getExecutorIds(): Seq[String] = lock.read {
    executorDataMap.keySet.toSeq
  }

  def getExecutorsWithRegistrationTs(): Map[String, Long] = lock.read {
    executorDataMap.mapValues(v => v.registrationTs).toMap
  }

  override def isExecutorActive(id: String): Boolean = lock.read {
    isExecutorAliveInner(id)
  }

  private def isExecutorAliveInner(id: String) = {
    executorDataMap.contains(id) && !isRemoving(id)
  }

  private def isRemoving(id: String) = {
    executorsPendingToRemove.contains(id) ||
      executorsPendingLossReason.contains(id) ||
      executorsPendingDecommission.contains(id)
  }

  /**
   * Get the max number of tasks that can be concurrent launched based on the ResourceProfile
   * could be used, even if some of them are being used at the moment.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @param rp ResourceProfile which to use to calculate max concurrent tasks.
   * @return The max number of tasks that can be concurrent launched currently.
   */
  override def maxNumConcurrentTasks(rp: ResourceProfile): Int = lock.read {
    val (rpIds, cpus, resources) = {
      executorDataMap
        .filterNot { case (id, _) => isRemoving(id) }
        .values.toArray.map { executor =>
          (
            executor.resourceProfileId,
            executor.totalCores,
            executor.resourcesInfo.map { case (name, rInfo) => (name, rInfo.totalAddressAmount) }
          )
        }.unzip3
    }
    TaskScheduler.calculateAvailableSlots(sc, rp.id, rpIds, cpus, resources)
  }

  // this function is for testing only
  def getExecutorAvailableResources(
      executorId: String): Map[String, ExecutorResourceInfo] = lock.read {
    executorDataMap.get(executorId).map(_.resourcesInfo).getOrElse(Map.empty)
  }

  // this function is for testing only
  def getExecutorResourceProfileId(executorId: String): Int = lock.read {
    val execDataOption = executorDataMap.get(executorId)
    execDataOption.map(_.resourceProfileId).getOrElse(ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID)
  }

  /**
   * Request an additional number of executors from the cluster manager. This is
   * requesting against the default ResourceProfile, we will need an API change to
   * allow against other profiles.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")

    val response = lock.write {
      val defaultProf = sc.resourceProfileManager.defaultResourceProfile
      val numExisting = requestedTotalExecutorsPerResourceProfile.getOrElse(defaultProf, 0)
      requestedTotalExecutorsPerResourceProfile(defaultProf) = numExisting + numAdditionalExecutors
      // Account for executors pending to be added or removed
      doRequestTotalExecutors(
        requestedTotalExecutorsPerResourceProfile.toMap,
        numLocalityAwareTasksPerResourceProfileId,
        rpHostToLocalTaskCount)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param resourceProfileIdToNumExecutors The total number of executors we'd like to have per
   *                                      ResourceProfile. The cluster manager shouldn't kill any
   *                                      running executor to reach this number, but, if all
   *                                      existing executors were to die, this is the number
   *                                      of executors we'd want to be allocated.
   * @param numLocalityAwareTasksPerResourceProfileId The number of tasks in all active stages that
   *                                                  have a locality preferences per
   *                                                  ResourceProfile. This includes running,
   *                                                  pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  final override def requestTotalExecutors(
      resourceProfileIdToNumExecutors: Map[Int, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]
  ): Boolean = {
    logInfo(s"requestTotalExecutors" +
      s" resourceProfileIdToNumExecutors=${resourceProfileIdToNumExecutors.size}" +
      s" numLocalityAwareTasksPerResourceProfileId=" +
      s"${numLocalityAwareTasksPerResourceProfileId.size}" +
      s" hostToLocalTaskCount=${hostToLocalTaskCount.map {
        case (k, v) => k -> v.size}.mkString(", " )
      }")

    val totalExecs = resourceProfileIdToNumExecutors.values.sum
    if (totalExecs < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$totalExecs from the cluster manager. Please specify a positive number!")
    }
    val resourceProfileToNumExecutors = resourceProfileIdToNumExecutors.map { case (rpid, num) =>
      (sc.resourceProfileManager.resourceProfileFromId(rpid), num)
    }
    val response = lock.write {
      this.requestedTotalExecutorsPerResourceProfile.clear()
      this.requestedTotalExecutorsPerResourceProfile ++= resourceProfileToNumExecutors
      this.numLocalityAwareTasksPerResourceProfileId = numLocalityAwareTasksPerResourceProfileId
      this.rpHostToLocalTaskCount = hostToLocalTaskCount
      doRequestTotalExecutors(
        requestedTotalExecutorsPerResourceProfile.toMap,
        numLocalityAwareTasksPerResourceProfileId,
        rpHostToLocalTaskCount)
    }
    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return a future whose evaluation indicates whether the request is acknowledged.
   */
  override protected def doRequestTotalExecutors(
     resourceProfileToTotalExecs: Map[ResourceProfile, Int],
     numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
     hostToLocalTaskCount: Map[Int, Map[String, Int]]): Future[Boolean] =
    Future.successful(false)

  /**
   * Adjust the number of executors being requested to no longer include the provided executors.
   */
  private def adjustExecutors(executorIds: Seq[String]) = {
    if (executorIds.nonEmpty) {
      executorIds.foreach { exec =>
        withLock {
          val rpId = executorDataMap(exec).resourceProfileId
          val rp = sc.resourceProfileManager.resourceProfileFromId(rpId)
          if (requestedTotalExecutorsPerResourceProfile.isEmpty) {
            // Assume that we are killing an executor that was started by default and
            // not through the request api
            requestedTotalExecutorsPerResourceProfile(rp) = 0
          } else {
            val requestedTotalForRp = requestedTotalExecutorsPerResourceProfile(rp)
            requestedTotalExecutorsPerResourceProfile(rp) = math.max(requestedTotalForRp - 1, 0)
          }
        }
      }
      doRequestTotalExecutors(
        requestedTotalExecutorsPerResourceProfile.toMap,
        numLocalityAwareTasksPerResourceProfileId,
        rpHostToLocalTaskCount)
    } else {
      Future.successful(true)
    }
  }

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * @param executorIds identifiers of executors to kill
   * @param adjustTargetNumExecutors whether the target number of executors be adjusted down
   *                                 after these executors have been killed
   * @param countFailures if there are tasks running on the executors when they are killed, whether
   *                      those failures be counted to task failure limits?
   * @param force whether to force kill busy executors, default false
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  final override def killExecutors(
      executorIds: Seq[String],
      adjustTargetNumExecutors: Boolean,
      countFailures: Boolean,
      force: Boolean): Seq[String] = {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")

    val response =
      withLock {
        val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
        unknownExecutors.foreach { id =>
          logWarning(s"Executor to kill $id does not exist!")
        }

        // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
        // If this executor is busy, do not kill it unless we are told
        // to force kill it (SPARK-9552)
        val executorsToKill = knownExecutors
          .filter { id => !executorsPendingToRemove.contains(id) }
          .filter { id => force || !taskScheduler.isExecutorBusy(id) }
        executorsToKill.foreach { id => executorsPendingToRemove(id) = !countFailures }

        logInfo(s"Actual list of executor(s) to be killed is ${executorsToKill.mkString(", ")}")

        // If we do not wish to replace the executors we kill, sync the target number of executors
        // with the cluster manager to avoid allocating new ones. When computing the new target,
        // take into account executors that are pending to be added or removed.
        val adjustTotalExecutors =
          if (adjustTargetNumExecutors) {
            adjustExecutors(executorsToKill)
          } else {
            Future.successful(true)
          }

        val killExecutors: Boolean => Future[Boolean] =
          if (executorsToKill.nonEmpty) {
            _ => doKillExecutors(executorsToKill)
          } else {
            _ => Future.successful(false)
          }

        val killResponse = adjustTotalExecutors.flatMap(killExecutors)(ThreadUtils.sameThread)

        killResponse.flatMap(killSuccessful =>
          Future.successful (if (killSuccessful) executorsToKill else Seq.empty[String])
        )(ThreadUtils.sameThread)
      }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  override protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)

  /**
   * Request that the cluster manager kill all executors on a given host.
   * @return whether the kill request is acknowledged.
   */
  final override def killExecutorsOnHost(host: String): Boolean = {
    logInfo(s"Requesting to kill any and all executors on host ${host}")
    // A potential race exists if a new executor attempts to register on a host
    // that is on the exclude list and is no no longer valid. To avoid this race,
    // all executor registration and killing happens in the event loop. This way, either
    // an executor will fail to register, or will be killed when all executors on a host
    // are killed.
    // Kill all the executors on this host in an event loop to ensure serialization.
    driverEndpoint.send(KillExecutorsOnHost(host))
    true
  }

  /**
   * Called when a new set of delegation tokens is sent to the driver. Child classes can override
   * this method but should always call this implementation, which handles token distribution to
   * executors.
   */
  override def updateDelegationTokens(tokens: Array[Byte]): Unit = {
    SparkHadoopUtil.get.addDelegationTokens(tokens, conf)
    delegationTokens.set(tokens)
    executorDataMap.values.foreach { ed =>
      ed.executorEndpoint.send(UpdateDelegationTokens(tokens))
    }
  }

  /**
   * Checks whether the executor is excluded due to failure(s). This is called when the executor
   * tries to register with the scheduler, and will deny registration if this method returns true.
   *
   * This is in addition to the exclude list kept by the task scheduler, so custom implementations
   * don't need to check there.
   */
  protected def isExecutorExcluded(executorId: String, hostname: String): Boolean = false

  override def getTotalRegisteredExecutors(): Int = totalRegisteredExecutors.get()

  override def lastAllocatedExecutorId: Int = currentExecutorIdCounter

  override def onDisconnectedExecutor(rpcAddress: RpcAddress, executorId: String): Unit = {
    removeExecutor(executorId,
      ExecutorProcessLost("Remote RPC client disassociated. Likely due to " +
        "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
        "messages."))
  }

  // SPARK-27112: We need to ensure that there is ordering of lock acquisition
  // between TaskSchedulerImpl and CoarseGrainedSchedulerBackend objects in order to fix
  // the deadlock issue exposed in SPARK-27112
  private def withLock[T](fn: => T): T = {
    taskScheduler.writeLock {
      lock.write {
        fn
      }
    }
  }

}

private[spark] object CoarseGrainedSchedulerBackend extends Logging {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
  val DEFAULT_DECOMMISSIONING_TIMEOUT_THRESHOLD_SECS = 20

  def endpointName(id: Option[Int]): String = {
    val name = ENDPOINT_NAME + id.map(i => s"_$i").getOrElse("")

    logInfo(s"Creating endpoint name: $name")

    name
  }

  def apply(sparkContext: SparkContext,
            scheduler: TaskScheduler,
            idOpt: Option[Int] = None): CoarseGrainedSchedulerBackend = {
    new CoarseGrainedSchedulerBackend {
      override def sc: SparkContext = sparkContext
      override def taskScheduler: TaskScheduler = scheduler
      override def id: Option[Int] = idOpt
    }
  }
}
