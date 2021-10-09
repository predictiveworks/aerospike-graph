package de.kp.works.aerospike
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.aerospike.client.AerospikeException.ScanTerminated
import com.aerospike.client.policy.ScanPolicy
import com.aerospike.client.{AerospikeClient, Key, Record, ScanCallback}
import de.kp.works.aerospike.util.NamedThreadFactory

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util

case class KeyRecord(key:Key, record:Record)

class AeroScanIterator() extends ScanCallback with util.Iterator[KeyRecord] {

  private var thread:Thread = _
  /*
   * A blocking queue to collect (key, record) pairs
   * from Aerospike while scanning.
   */
  private val queue = new LinkedBlockingQueue[KeyRecord](100)
  private var nextKeyRecord:KeyRecord = _

  private val closed = new AtomicBoolean(false)
  private val TERMINATE_VALUE = KeyRecord(null, null)

  def setThread(thread:Thread):Unit = {
    this.thread = thread
  }

  def close(): Unit = {

    closed.set(true)
    try {

      queue.put(TERMINATE_VALUE)
      thread.interrupt()

    } catch {
      case _: InterruptedException =>
        throw new RuntimeException
    }

  }

  override def scanCallback(key: Key, record: Record): Unit = {

    try {
      queue.put(KeyRecord(key, record))

    } catch {
      case t:Throwable =>
        throw new ScanTerminated(t)
    }

  }

  override def hasNext: Boolean = {

    try {

      if (queue.size() == 0) false
      else
       if (nextKeyRecord == TERMINATE_VALUE) false else true

    } catch {
      case t: Throwable =>
        throw new RuntimeException(t)
    }

  }

  override def next(): KeyRecord = {

    try {

      nextKeyRecord = {

        val keyRecord = queue.take()
        if (keyRecord == TERMINATE_VALUE)
          null

        else {
          keyRecord
        }

      }



    } finally {
      nextKeyRecord = TERMINATE_VALUE
    }

    nextKeyRecord

  }

}

class AeroScan(
  client:AerospikeClient,
  namespace:String,
  setname:String,
  readTimeout:Int,
  socketTimeout:Int) {
  /*
   * The AeroScan implements a parallel scanning
   * approach
   */
  private val scanPolicy = new ScanPolicy()
  scanPolicy.sendKey = true
  scanPolicy.includeBinData = true

  scanPolicy.socketTimeout = socketTimeout
  scanPolicy.totalTimeout = readTimeout

  private val scanIterator = new AeroScanIterator()

  def run(scanThreadFactory:NamedThreadFactory): util.Iterator[KeyRecord] = {

    val scanThread = scanThreadFactory.newThread(
      new Runnable() {
        override def run(): Unit = {
          try {

            /* Run scan operation for the provided namespace & set */
            client.scanAll(scanPolicy, namespace, setname, scanIterator)

          } catch {
            case t:Throwable => throw t

          } finally {
            scanIterator.close()
          }
        }
      }
    )

    scanIterator.setThread(scanThread)
    scanThread.start()

    scanIterator

  }

}
