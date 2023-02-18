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

import java.util.concurrent.TimeUnit

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config

trait SchedulerBackendCommon { this: SchedulerBackend =>
  private val createTimeNs = System.nanoTime()

  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  lazy val maxRegisteredWaitingTimeNs: Long = TimeUnit.MILLISECONDS.toNanos(
    conf.get(config.SCHEDULER_MAX_REGISTERED_RESOURCE_WAITING_TIME))

  override def start(): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      delegationTokenManager = createTokenManager()
      delegationTokenManager.foreach { dtm =>
        val ugi = UserGroupInformation.getCurrentUser()
        val tokens = if (dtm.renewalEnabled) {
          dtm.start()
        } else {
          val creds = ugi.getCredentials()
          dtm.obtainDelegationTokens(creds)
          if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
            SparkHadoopUtil.get.serialize(creds)
          } else {
            null
          }
        }
        if (tokens != null) {
          updateDelegationTokens(tokens)
        }
      }
    }
  }

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered()) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.nanoTime() - createTimeNs) >= maxRegisteredWaitingTimeNs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeNs(ns)")
      return true
    }
    false
  }
}
