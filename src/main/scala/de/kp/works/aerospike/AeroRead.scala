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

import com.aerospike.client.{AerospikeClient, Record}
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.query.{Filter, Statement}
import de.kp.works.aerospike.gremlin.Constants

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConversions._

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
    var remaining = Seq.empty[AeroFilter]

    if (filters.nonEmpty) {
      filters.condition match {
        case "and" =>
          /*
           * Extract the remaining filter conditions
           * that must be applied to the query result
           */
          remaining =
            if (filters.size < 2) Seq.empty[AeroFilter] else filters.tail
          /*
           * In case of an `and` condition we assign
           * the first (head) filter to query statement
           */
          val filter = filters.head
          filter.condition match {
            case Constants.EQUAL_VALUE =>
              val f = Filter.equal(filter.name, filter.value)
              stmt.setFilter(f)
             case _ =>
              throw new Exception(s"Filter condition `${filter.condition} is not supported.")
          }
        case "or" =>
          /*
           * In case of and `or` condition, we cannot
           * assign any filter to the statement, but
           * must apply all condition to the query result
           */
        case _ =>
          throw new Exception(s"Filters condition `${filters.condition} is not supported.")

      }
    }

    if (binNames.nonEmpty) stmt.setBinNames(binNames: _*)
    /*
     * Retrieve read cursor
     */
    val rs = client.query(queryPolicy, stmt)
    /*
     * Evaluate cursor and thereby mimic limitation
     */
    val limited = filters.limit != -1
    val limit = filters.limit
    /*
     * Flag to indicate whether the specified
     * limit has been reached
     */
    var terminate = false
    try {

      while (rs.next() && !terminate) {
        /*
         * Check whether we have to terminate
         * due to provided limitations
         */
        if (limited && readIterator.size == limit)
          terminate = true

        if (!terminate) {
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
                val matches = remaining
                  .map(filter => applyFilter(record, filter))

                if (matches.sum == 0)
                  readIterator.put(KeyRecord(key, record))

              case "or" =>
                /*
                 * The filter conditions have to be applied
                 * to the query result
                 */
                var matches = 0
                filters.filters.foreach(filter => {
                  filter.condition match {
                    case Constants.EQUAL_VALUE =>
                      if (record.getString(filter.name) == filter.name)
                        matches += 1

                    case _ =>
                      throw new Exception(s"Filter condition `${filter.condition} is not supported.")
                  }

                })

                if (matches > 0)
                  readIterator.put(KeyRecord(key, record))

              case _ =>
                throw new Exception(s"Filters condition `${filters.condition} is not supported.")

            }
          }
          else
            readIterator.put(KeyRecord(key, record))
        }

      }
    } finally {
      rs.close()
      readIterator.close()

    }

    readIterator

  }

  private def applyFilter(record:Record, filter:AeroFilter):Int = {
    filter.condition match {
      case Constants.EQUAL_VALUE =>
        if (record.getString(filter.name) == filter.name)
          0
        else 1
      case Constants.INCLUSIVE_FROM_VALUE =>
        /* `filter.name` represents PROPERTY_VALUE_COL_NAME */
        val fieldType = getFieldType(record, filter.name)
        fieldType match {
          case "COUNTER" =>
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue >= filterValue) 0 else 1
          case "DATE" =>
            /*
             * 32-bit integer representing the
             * number of DAYS since Unix epoch
             */
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue >= filterValue) 0 else 1
          case "DECIMAL" =>
            val propValue = BigDecimal(record.getString(filter.name))
            val filterValue = BigDecimal(filter.value.toDouble)

            if (propValue >= filterValue) 0 else 1
          case "DOUBLE" =>
            val propValue = record.getString(filter.name).toDouble
            val filterValue = filter.value.toDouble

            if (propValue >= filterValue) 0 else 1
          case "FLOAT" =>
            val propValue = record.getString(filter.name).toFloat
            val filterValue = filter.value.toFloat

            if (propValue >= filterValue) 0 else 1
          case "INT" =>
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue >= filterValue) 0 else 1
          case "INTERVAL" =>
            /*
             * A value representing a period of
             * time between two instants.
             */
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue >= filterValue) 0 else 1
          case "LONG" =>
            val propValue = record.getString(filter.name).toLong
            val filterValue = filter.value.toLong

            if (propValue >= filterValue) 0 else 1
          case "SHORT" =>
            val propValue = record.getString(filter.name).toShort
            val filterValue = filter.value.toShort

            if (propValue >= filterValue) 0 else 1
          case "TIME" =>
            /*
             * 32-bit integer representing time
             * of the day in milliseconds.
             */
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue >= filterValue) 0 else 1
          case "TIMESTAMP" =>
            /*
             * 64-bit integer representing the
             * number of milliseconds since epoch
             */
            val propValue = record.getString(filter.name).toLong
            val filterValue = filter.value.toLong

            if (propValue >= filterValue) 0 else 1
          case _ =>
            throw new Exception(s"The field type is not a number.")
        }
      case Constants.EXCLUSIVE_TO_VALUE =>
        /* `filter.name` represents PROPERTY_VALUE_COL_NAME */
        val fieldType = getFieldType(record, filter.name)
        fieldType match {
          case "COUNTER" =>
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue < filterValue) 0 else 1
          case "DATE" =>
            /*
             * 32-bit integer representing the
             * number of DAYS since Unix epoch
             */
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue < filterValue) 0 else 1
          case "DECIMAL" =>
            val propValue = BigDecimal(record.getString(filter.name))
            val filterValue = BigDecimal(filter.value.toDouble)

            if (propValue < filterValue) 0 else 1
          case "DOUBLE" =>
            val propValue = record.getString(filter.name).toDouble
            val filterValue = filter.value.toDouble

            if (propValue < filterValue) 0 else 1
          case "FLOAT" =>
            val propValue = record.getString(filter.name).toFloat
            val filterValue = filter.value.toFloat

            if (propValue < filterValue) 0 else 1
          case "INT" =>
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue < filterValue) 0 else 1
          case "INTERVAL" =>
            /*
             * A value representing a period of
             * time between two instants.
             */
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue < filterValue) 0 else 1
          case "LONG" =>
            val propValue = record.getString(filter.name).toLong
            val filterValue = filter.value.toLong

            if (propValue < filterValue) 0 else 1
          case "SHORT" =>
            val propValue = record.getString(filter.name).toShort
            val filterValue = filter.value.toShort

            if (propValue < filterValue) 0 else 1
          case "TIME" =>
            /*
             * 32-bit integer representing time
             * of the day in milliseconds.
             */
            val propValue = record.getString(filter.name).toInt
            val filterValue = filter.value.toInt

            if (propValue < filterValue) 0 else 1
          case "TIMESTAMP" =>
            /*
             * 64-bit integer representing the
             * number of milliseconds since epoch
             */
            val propValue = record.getString(filter.name).toLong
            val filterValue = filter.value.toLong

            if (propValue < filterValue) 0 else 1
          case _ =>
            throw new Exception(s"The field type is not a number.")
        }
      case _ =>
        throw new Exception(s"Filter condition `${filter.condition} is not supported.")

    }

  }

  private def getFieldType(record:Record, field:String):String = {

    field match {
      case Constants.ID_COL_NAME =>
        record.getString(Constants.ID_TYPE_COL_NAME)
      case Constants.LABEL_COL_NAME =>
        "STRING"
      case Constants.TO_COL_NAME =>
        record.getString(Constants.TO_TYPE_COL_NAME)
      case Constants.FROM_COL_NAME =>
        record.getString(Constants.FROM_TYPE_COL_NAME)
      case Constants.CREATED_AT_COL_NAME =>
        "LONG"
      case Constants.UPDATED_AT_COL_NAME =>
        "LONG"
      case Constants.PROPERTY_KEY_COL_NAME =>
        record.getString(Constants.PROPERTY_TYPE_COL_NAME)
      case Constants.PROPERTY_TYPE_COL_NAME =>
        "STRING"
      case Constants.PROPERTY_VALUE_COL_NAME =>
        record.getString(Constants.PROPERTY_TYPE_COL_NAME)
      case _ =>
        throw new Exception(s"The field `$field` is not supported.")
    }

  }
}
