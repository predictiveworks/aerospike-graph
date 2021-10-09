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

public class AeroEdgesWithLimitQuery extends AeroQuery {

    public AeroEdgesWithLimitQuery(String cacheName, AeroConnect connect,
                                   Object vertex, Direction direction, String label,
                                   String key, Object InclusiveFromValue, int limit, boolean reversed) {
        super(cacheName, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(Constants.LABEL_COL_NAME, label);
        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);

        fields.put(Constants.INCLUSIVE_FROM_VALUE, InclusiveFromValue.toString());
        fields.put(Constants.LIMIT_VALUE, String.valueOf(limit));

        fields.put(Constants.REVERSED_VALUE, String.valueOf(reversed));

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
//             * INCLUSIVE_FROM_VALUE >= PROPERTY_VALUE_COL_NAME
//             */
//            sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
//            sqlStatement += " >= '" + fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE) + "'";
//            /*
//             * Determine sorting order
//             */
//            if (fields.get(IgniteConstants.REVERSED_VALUE).equals("true")) {
//                sqlStatement += " order by " + IgniteConstants.PROPERTY_VALUE_COL_NAME + " DESC";
//            }
//            else {
//                sqlStatement += " order by " + IgniteConstants.PROPERTY_VALUE_COL_NAME + " ASC";
//            }
//
//            sqlStatement += " limit " + fields.get(IgniteConstants.LIMIT_VALUE);
//
//        } catch (Exception e) {
//            sqlStatement = null;
//        }
//
//    }
}
