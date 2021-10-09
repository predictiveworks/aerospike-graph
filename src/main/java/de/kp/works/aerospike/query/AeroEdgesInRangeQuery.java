package de.kp.works.aerospike.query;
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

import de.kp.works.aerospike.AeroConnect;
import de.kp.works.aerospike.KeyRecord;
import de.kp.works.aerospikegraph.Constants;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.Iterator;

public class AeroEdgesInRangeQuery extends AeroQuery {
    /**
     * This query is restricted to the Ignite cache that
     * contains the edges of the graph network.
     */
    public AeroEdgesInRangeQuery(String cacheName, AeroConnect connect,
                                 Object vertex, Direction direction, String label,
                                 String key, Object inclusiveFromValue, Object exclusiveToValue) {
        super(cacheName, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(Constants.LABEL_COL_NAME, label);
        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);

        fields.put(Constants.INCLUSIVE_FROM_VALUE, inclusiveFromValue.toString());
        fields.put(Constants.EXCLUSIVE_TO_VALUE, exclusiveToValue.toString());

    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        return null;
    }

//    @Override
//    protected void createSql(Map<String, String> fields) {
//        try {
//            buildSelectPart();
//            /*
//             * Build `where` clause and thereby distinguish
//             * between a single or multiple label values
//             */
//            if (fields.containsKey(IgniteConstants.TO_COL_NAME)) {
//                sqlStatement += " where " + IgniteConstants.TO_COL_NAME;
//                sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "'";
//            }
//            if (fields.containsKey(IgniteConstants.FROM_COL_NAME)) {
//                sqlStatement += " where " + IgniteConstants.FROM_COL_NAME;
//                sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";
//            }
//
//            sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
//            sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";
//
//            sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
//            sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";
//            /*
//             * The value of the value column must in the range of
//             * INCLUSIVE_FROM_VALUE >= PROPERTY_VALUE_COL_NAME < EXCLUSIVE_TO_VALUE
//             */
//            sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
//            sqlStatement += " >= '" + fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE) + "'";
//
//            sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
//            sqlStatement += " < '" + fields.get(IgniteConstants.EXCLUSIVE_TO_VALUE) + "'";
//
//            sqlStatement += " order by " + IgniteConstants.PROPERTY_VALUE_COL_NAME + " ASC";
//
//        } catch (Exception e) {
//            sqlStatement = null;
//        }
//
//    }
}