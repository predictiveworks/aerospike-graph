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

import com.aerospike.client.AerospikeClient
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.query.{Filter, Statement}

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

class AeroReadIterator extends util.Iterator[KeyRecord] {
  /*
   * A blocking queue to collect (key, record) pairs
   * from Aerospike while reading (or querying).
   */
  private val queue = new LinkedBlockingQueue[KeyRecord](100)
  private var nextKeyRecord:KeyRecord = _

  private val closed = new AtomicBoolean(false)
  private val TERMINATE_VALUE = KeyRecord(null, null)

  def close(): Unit = {

    closed.set(true)
    try {
     queue.put(TERMINATE_VALUE)

    } catch {
      case _: InterruptedException =>
        throw new RuntimeException
    }

  }

  def put(keyRecord:KeyRecord):Unit = {
    queue.put(keyRecord)
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
/**
 * AeroRead queries an Aerospike database
 * using secondary indexes.
 */
class AeroRead(
  client:AerospikeClient,
  namespace:String,
  setname:String,
  readTimeout:Int,
  socketTimeout:Int) {

  private val stmt = new Statement()
  stmt.setNamespace(namespace)
  stmt.setSetName(setname)
  /*
   * Build query policy
   */
  private val queryPolicy = new QueryPolicy()
  queryPolicy.socketTimeout = socketTimeout
  queryPolicy.totalTimeout = readTimeout

  private val readIterator = new AeroReadIterator()

  def run(filters:AeroFilters, binNames:Seq[String] = Seq.empty[String]): util.Iterator[KeyRecord] = {
    /*
     * The query logic demands for records that have
     * both, from and to value equal to the specified
     * one.
     *
     * Aerospike (see documentation) currently only
     * supports a single filter for queries.
     */
    val remaining =
      if (filters.size < 2) Seq.empty[AeroFilter] else filters.tail

    if (filters.nonEmpty) {
      /*
       * Build Aerospike filter
       */
      val filter = filters.head
      filter.condition match {
        case "equal" =>
          val f = Filter.equal(filter.name, filter.value)
          stmt.setFilter(f)
        case _ =>
          throw new Exception(s"Filter condition `${filter.condition} is not supported.")
      }

    }
    if (binNames.nonEmpty) stmt.setBinNames(binNames: _*)
    /*
     * Retrieve read cursor
     */
    val rs = client.query(queryPolicy, stmt)
    try {

      while (rs.next()) {

        val key = rs.getKey
        val record = rs.getRecord
        /*
         * Check whether there are additional filters
         * that must be applied
         */
        if (remaining.nonEmpty) {
          /*
           * The received records are fulfill the primary
           * filter condition
           */
          filters.condition match {
            case "and" =>
              /*
               * Check whether all remaining conditions
               * are also fulfilled
               */
              val matches = remaining.map(filter => {
                filter.condition match {
                  case "equal" =>
                    if (record.getString(filter.name) == filter.name)
                      0
                    else 1

                  case _ =>
                    throw new Exception(s"Filter condition `${filter.condition} is not supported.")
                }
              })

              if (matches.sum == 0)
                readIterator.put(KeyRecord(key, record))
            case _ =>
              throw new Exception(s"Filters condition `${filters.condition} is not supported.")

          }
        }
        else
          readIterator.put(KeyRecord(key, record))

      }
    } finally {
      rs.close()
      readIterator.close()

    }

    readIterator

  }
}
