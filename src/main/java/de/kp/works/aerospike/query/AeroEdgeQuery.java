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

public class AeroEdgeQuery extends AeroQuery {
    /**
     * Retrieve the edge that refers to the provided
     * `from` and `to `identifier. A use case for this
     * query is the OpenCTI transformer
     */
    public AeroEdgeQuery(String cacheName, AeroConnect connect, Object fromId, Object toId) {
        super(cacheName, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();

        fields.put(Constants.FROM_COL_NAME, fromId.toString());
        fields.put(Constants.TO_COL_NAME,   toId.toString());

    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        return null;
    }

//    @Override
//    protected void createSql(Map<String, String> fields) {
//
//        try {
//            buildSelectPart();
//            /*
//             * Build the `where` clause and thereby distinguish
//             * between a single or multiple identifier values
//             */
//            sqlStatement += " where " + IgniteConstants.FROM_COL_NAME;
//            sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";
//
//            sqlStatement += " and " + IgniteConstants.TO_COL_NAME;
//            sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "'";
//
//        } catch (Exception e) {
//            sqlStatement = null;
//        }
//
//    }
}
