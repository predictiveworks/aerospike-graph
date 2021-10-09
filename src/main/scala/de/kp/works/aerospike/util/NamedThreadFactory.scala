package de.kp.works.aerospike.util
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

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
/**
 * A [ThreadFactory] with the ability to set the thread
 * name prefix. This class is exactly similar to
 *
 * java.util.concurrent.Executors#defaultThreadFactory()
 *
 * from JDK8, except for the thread naming feature.
 *
 * The factory creates threads that have names on the form
 * `prefix-N-thread-M`, where `prefix` is a string provided
 * in the constructor, N is the sequence number of this factory,
 * and M is the sequence number of the thread created by this
 * factory.
 */
class NamedThreadFactory(groupName:String, prefix:String) extends ThreadFactory {
  /*
   * Note:  The source code for this class was based entirely on
   * Executors.DefaultThreadFactory class from the JDK8 source.
   * The only change made is the ability to configure the thread
   * name prefix.
   */
  val secMan: SecurityManager = System.getSecurityManager
  val parent: ThreadGroup = if (secMan != null)
    secMan.getThreadGroup

  else Thread.currentThread().getThreadGroup

  private val group = new ThreadGroup(parent, groupName)
  private val namePrefix: String = groupName + "-" + prefix + "-"

  private val threadNumber = new AtomicInteger(1)

  override def newThread(r:Runnable):Thread = {

    val thread = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(),0)
    if (thread.isDaemon) thread.setDaemon(false)

    if (thread.getPriority != Thread.NORM_PRIORITY) {
      thread.setPriority(Thread.NORM_PRIORITY);
    }

    thread

  }

}
