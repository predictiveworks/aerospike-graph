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

public class AeroPropertyQuery extends AeroQuery {
    /**
     * Retrieves all elements that are referenced by
     * a certain label and share a certain property
     * key and value
     */
    public AeroPropertyQuery(String name, AeroConnect connect, String label, String key, Object value) {
        super(name, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();

        fields.put(Constants.LABEL_COL_NAME, label);
        
        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);
        fields.put(Constants.PROPERTY_VALUE_COL_NAME, value.toString());

    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        return null;
    }

//    @Override
//    protected void createSql(Map<String, String> fields) {
//        try {
//
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
//             * The value column must match the provided value
//             */
//            sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
//            sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME) + "'";
//
//        } catch (Exception e) {
//            sqlStatement = null;
//        }
//
//    }
}
