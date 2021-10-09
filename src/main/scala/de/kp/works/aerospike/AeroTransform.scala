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

import de.kp.works.aerospike.query.AeroResult
import de.kp.works.aerospikegraph.{Constants, ValueType}

import scala.collection.JavaConversions._

object AeroTransform {

  def transformEdgeEntries(entries: java.util.List[AeroEdgeEntry]): java.util.List[AeroResult] = {
    entries
      .groupBy(entry => entry.id)
      .map { case (_, values) =>

        val aeroResult = new AeroResult()
        /*
         * Extract common fields
         */
        val head = values.head
        val id: String = head.id

        val idType: String = head.idType
        val label: String = head.label

        val toId: String = head.toId
        val toIdType: String = head.toIdType

        val fromId: String = head.fromId
        val fromIdType: String = head.fromIdType

        val createdAt: String = head.createdAt.toString
        val updatedAt: String = head.updatedAt.toString
        /*
         * Add common fields
         */
        aeroResult
          .addColumn(Constants.ID_COL_NAME, idType, id)

        aeroResult
          .addColumn(Constants.LABEL_COL_NAME, ValueType.STRING.name(), label)

        aeroResult
          .addColumn(Constants.TO_COL_NAME, toIdType, toId)

        aeroResult
          .addColumn(Constants.FROM_COL_NAME, fromIdType, fromId)

        aeroResult
          .addColumn(Constants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt)

        aeroResult
          .addColumn(Constants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt)

        aeroResult

      }.toList

  }

  def transformVertexEntries(entries: java.util.List[AeroVertexEntry]): java.util.List[AeroResult] = {
    entries
      .groupBy(entry => entry.id)
      .map { case (_, values) =>

        val aeroResult = new AeroResult()
        /*
         * Extract common fields
         */
        val head = values.head
        val id: String = head.id

        val idType: String = head.idType
        val label: String = head.label

        val createdAt: String = head.createdAt.toString
        val updatedAt: String = head.updatedAt.toString
        /*
         * Add common fields
         */
        aeroResult
          .addColumn(Constants.ID_COL_NAME, idType, id)

        aeroResult
          .addColumn(Constants.LABEL_COL_NAME, ValueType.STRING.name(), label)

        aeroResult
          .addColumn(Constants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt)

        aeroResult
          .addColumn(Constants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt)
        /*
         * Extract & add properties
         */
        values.foreach(value => {

          val propKey: String = value.propKey
          val propType: String = value.propType
          val propValue: String = value.propValue

          aeroResult
            .addColumn(propKey, propType, propValue)

        })

        aeroResult

      }.toList

  }

}
