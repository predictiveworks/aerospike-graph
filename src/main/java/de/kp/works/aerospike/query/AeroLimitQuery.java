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

import java.util.HashMap;
import java.util.Iterator;

public class AeroLimitQuery extends AeroQuery {

    private String queryType;
    /**
     * Retrieves a specified number of (ordered) elements
     * from the beginning of the cache. Note, this query
     * is restricted to elements with a numeric identifier.
     */
    public AeroLimitQuery(String name, AeroConnect connect, int limit) {
        super(name, connect);
        /*
         * This query works for index `id` fields, either
         * for edges or vertices
         */
        fields = new HashMap<>();
        fields.put(Constants.LIMIT_VALUE, String.valueOf(limit));

        queryType = "withId";
    }

    public AeroLimitQuery(String name, AeroConnect connect, Object fromId, int limit) {
        super(name, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();

        fields.put(Constants.FROM_ID_VALUE, fromId.toString());
        fields.put(Constants.LIMIT_VALUE, String.valueOf(limit));

        queryType = "withFrom";

    }

    public AeroLimitQuery(String name, AeroConnect connect,
                          String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        super(name, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();

        fields.put(Constants.LABEL_COL_NAME, label);
        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);

        fields.put(Constants.INCLUSIVE_FROM_VALUE, inclusiveFrom.toString());
        fields.put(Constants.LIMIT_VALUE, String.valueOf(limit));

        fields.put(Constants.REVERSED_VALUE, String.valueOf(reversed));

        queryType = "withProp";
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
//             * Build the `clause` of the SQL statement
//             * from the provided fields
//             */
//            sqlStatement += " where " + IgniteConstants.LABEL_COL_NAME;
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
