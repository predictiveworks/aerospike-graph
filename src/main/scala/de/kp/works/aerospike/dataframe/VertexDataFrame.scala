package de.kp.works.aerospike.dataframe
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

import de.kp.works.aerospike.hadoop.{AeroInputFormat, AeroKey, AeroRecord}
import de.kp.works.aerospike.gremlin.{Constants, ValueType}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class VertexDataFrame(session:SparkSession, conf:Configuration) {

  private val sc = session.sparkContext

  def vertices():DataFrame = {

    val dataframe = read()
    /*
     * The dataframe contains the vertex entries that
     * are aggregated to vertices; as a result, all
     * properties of a vertex are aggregated into
     * a list of [Row].
     */
    val aggCols = Seq(
      Constants.PROPERTY_KEY_COL_NAME,
      Constants.PROPERTY_TYPE_COL_NAME,
      Constants.PROPERTY_VALUE_COL_NAME)

    val groupCols = Seq(
      Constants.ID_COL_NAME,
      Constants.ID_TYPE_COL_NAME,
      Constants.LABEL_COL_NAME,
      Constants.CREATED_AT_COL_NAME,
      Constants.UPDATED_AT_COL_NAME)

    val aggStruct = struct(aggCols.map(col): _*)
    var output = dataframe
      .groupBy(groupCols.map(col): _*)
      .agg(collect_list(aggStruct).as("properties"))
    /*
     * As a final step, the `id` column is transformed
     * into the right data type
     */
    val idType = output
      .select(Constants.ID_TYPE_COL_NAME)
      .head.getAs[String](0)

    val toLong = udf((id:String) => id.toLong)
    if (idType == ValueType.LONG.name()) {
      output = output.withColumn(Constants.ID_COL_NAME, toLong(col(Constants.ID_COL_NAME)))
    }

    output.drop(Constants.ID_TYPE_COL_NAME)
  }

  /**
   * This method reads all vertices as [AeroVertexEntry]
   * format and returns them as a DataFrame
   */
  def read():DataFrame =
    session.createDataFrame(load(), schema())

  /**
   * This method loads all vertices as [AeroVertexEntry]
   * format, which is more or less schema agnostic with
   * respect to associated properties.
   *
   * Grouping vertex entries with respect to the vertex
   * unique identifier (id) is recommended in combination
   * with vertex type filtering.
   */
  private def load():RDD[Row] = {

    try {

      val source = sc.newAPIHadoopRDD(conf, classOf[AeroInputFormat], classOf[AeroKey], classOf[AeroRecord])
      source.map{case(aeroKey,aeroRecord) =>

        val userKey = aeroKey.userKey.toString
        /*
         * 0 : User key
         *
         * 1 : Constants.ID_COL_NAME (String)
         * 2 : Constants.ID_TYPE_COL_NAME (String)
         * 3 : Constants.LABEL_COL_NAME (String)
         * 4 : Constants.CREATED_AT_COL_NAME (Long)
         * 5 : Constants.UPDATED_AT_COL_NAME (Long)
         * 6 : Constants.PROPERTY_KEY_COL_NAME (String)
         * 7 : Constants.PROPERTY_TYPE_COL_NAME (String)
         * 8 : Constants.PROPERTY_VALUE_COL_NAME (String)
         */
        val record = aeroRecord.toRecord
        val id = record.getString(Constants.ID_COL_NAME)

        val idType = record.getString(Constants.ID_TYPE_COL_NAME)
        val label  = record.getString(Constants.LABEL_COL_NAME)

        val createdAt  = record.getLong(Constants.CREATED_AT_COL_NAME)
        val updatedAt  = record.getLong(Constants.UPDATED_AT_COL_NAME)

        val propKey   = record.getString(Constants.PROPERTY_KEY_COL_NAME)
        val propType  = record.getString(Constants.PROPERTY_TYPE_COL_NAME)
        val propValue = record.getString(Constants.PROPERTY_VALUE_COL_NAME)

        val values = Seq(
          userKey,
          id,
          idType,
          label,
          createdAt,
          updatedAt,
          propKey,
          propType,
          propValue)

        Row.fromSeq(values)

      }

    } catch {
      case _:Throwable => null
    }

  }

  private def schema():StructType = {
    StructType(
      StructField(Constants.USER_KEY, StringType, nullable = false) ::
      StructField(Constants.ID_COL_NAME, StringType, nullable = false) ::
      StructField(Constants.ID_TYPE_COL_NAME, StringType, nullable = false) ::
      StructField(Constants.LABEL_COL_NAME, StringType, nullable = false) ::
      StructField(Constants.CREATED_AT_COL_NAME, LongType, nullable = false) ::
      StructField(Constants.UPDATED_AT_COL_NAME, LongType, nullable = false) ::
      StructField(Constants.PROPERTY_KEY_COL_NAME, StringType, nullable = true) ::
      StructField(Constants.PROPERTY_TYPE_COL_NAME, StringType, nullable = true) ::
      StructField(Constants.PROPERTY_VALUE_COL_NAME, StringType, nullable = true) :: Nil
    )
  }
}
