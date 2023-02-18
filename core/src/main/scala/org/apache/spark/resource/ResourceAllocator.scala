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

package org.apache.spark.resource

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.HashMap

import org.apache.spark.SparkException

/**
 * Trait used to help executor/worker allocate resources.
 * Please note that this is intended to be used in a single thread.
 */
private[spark] trait ResourceAllocator {

  protected def resourceName: String
  protected def resourceAddresses: Seq[String]
  protected def slotsPerAddress: Int

  /**
   * Map from an address to its availability, a value > 0 means the address is available,
   * while value of 0 means the address is fully assigned.
   *
   * For task resources ([[org.apache.spark.scheduler.ExecutorResourceInfo]]), this value
   * can be a multiple, such that each address can be allocated up to [[slotsPerAddress]]
   * times.
   *
   * TODO Use [[org.apache.spark.util.collection.OpenHashMap]] instead to gain better performance.
   */
  private lazy val addressAvailabilityMap: Map[String, AtomicInteger] = {
    HashMap(resourceAddresses.map(_ -> new AtomicInteger(slotsPerAddress)): _*)
  }

  /**
   * Sequence of currently available resource addresses.
   *
   * With [[slotsPerAddress]] greater than 1, [[availableAddrs]] can contain duplicate addresses
   * e.g. with [[slotsPerAddress]] == 2, availableAddrs for addresses 0 and 1 can look like
   * Seq("0", "0", "1"), where address 0 has two assignments available, and 1 has one.
   */
  def availableAddrs: Seq[String] = addressAvailabilityMap
    .flatMap { case (addr, available) =>
      (0 until available.get()).map(_ => addr)
    }.toSeq.sorted

  /**
   * Sequence of currently assigned resource addresses.
   *
   * With [[slotsPerAddress]] greater than 1, [[assignedAddrs]] can contain duplicate addresses
   * e.g. with [[slotsPerAddress]] == 2, assignedAddrs for addresses 0 and 1 can look like
   * Seq("0", "1", "1"), where address 0 was assigned once, and 1 was assigned twice.
   */
  private[spark] def assignedAddrs: Seq[String] = addressAvailabilityMap
    .flatMap { case (addr, available) =>
      (0 until slotsPerAddress - available.get()).map(_ => addr)
    }.toSeq.sorted

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   * Throw an Exception if an address is not available or doesn't exist.
   */
  def acquire(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      addressAvailabilityMap.get(address) match {
        case Some(available) =>
          val prev =
            available.getAndUpdate { n =>
              if (n > 0) {
                n - 1
              } else {
                n
              }
            }

          if (prev <= 0) {
            throw new SparkException("Try to acquire an address that is not available. " +
              s"$resourceName address $address is not available.")
          }
        case _ =>
          throw new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
            s"address $address doesn't exist.")
      }
    }
  }

  /**
   * Release a sequence of resource addresses, these addresses must have been assigned. Resource
   * addresses are released when a task has finished.
   * Throw an Exception if an address is not assigned or doesn't exist.
   */
  def release(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      addressAvailabilityMap.get(address) match {
        case Some(available) =>
          val slots = slotsPerAddress

          val prev =
            available.getAndUpdate { n =>
              if (n < slots) {
                n + 1
              } else {
                n
              }
            }

          if (prev >= slots) {
            throw new SparkException(s"Try to release an address that is not assigned." +
              s" $resourceName address $address is not assigned.")
          }
        case _ =>
          throw new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
            s"address $address doesn't exist.")
      }
    }
  }
}
