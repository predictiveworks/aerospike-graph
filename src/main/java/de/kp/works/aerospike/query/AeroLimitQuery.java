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
import de.kp.works.aerospike.AeroFilter;
import de.kp.works.aerospike.AeroFilters;
import de.kp.works.aerospike.KeyRecord;
import de.kp.works.aerospike.gremlin.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class AeroLimitQuery extends AeroQuery {

    private final String queryType;
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

        fields.put(Constants.FROM_COL_NAME, fromId.toString());
        fields.put(Constants.LIMIT_VALUE, String.valueOf(limit));

        queryType = "withFrom";

    }

    public AeroLimitQuery(String name, AeroConnect connect,
                          String label, String key, Object inclusiveFrom, int limit) {
        super(name, connect);

        fields = new HashMap<>();

        fields.put(Constants.LABEL_COL_NAME, label);
        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);

        fields.put(Constants.INCLUSIVE_FROM_VALUE, inclusiveFrom.toString());
        fields.put(Constants.LIMIT_VALUE, String.valueOf(limit));

        queryType = "withProp";
    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {

        int limit = Integer.parseInt(fields.get(Constants.LIMIT_VALUE));
        List<AeroFilter> filters = new ArrayList<>();

        if (queryType.equals("withId")) {
            return connect
                    .query(setname, new AeroFilters("and", filters, limit));
        }
        else if (queryType.equals("withFrom")) {
            filters.add(
                    new AeroFilter(Constants.EQUAL_VALUE, Constants.FROM_COL_NAME,
                            fields.get(Constants.FROM_COL_NAME)));

            return connect
                    .query(setname, new AeroFilters("and", filters, limit));
        }
        else {
            filters.add(
                    new AeroFilter(Constants.EQUAL_VALUE, Constants.LABEL_COL_NAME,
                            fields.get(Constants.LABEL_COL_NAME)));

            filters.add(
                    new AeroFilter(Constants.EQUAL_VALUE, Constants.PROPERTY_KEY_COL_NAME,
                            fields.get(Constants.PROPERTY_KEY_COL_NAME)));

            filters.add(
                    new AeroFilter(Constants.INCLUSIVE_FROM_VALUE, Constants.PROPERTY_VALUE_COL_NAME,
                            fields.get(Constants.INCLUSIVE_FROM_VALUE)));

            return connect
                    .query(setname, new AeroFilters("and", filters, limit));

        }

    }

}
