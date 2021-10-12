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
        /*
         * The query logic demands for records that have
         * the specified label as well as the property
         */
        List<AeroFilter> filters = new ArrayList<>();
        filters.add(
                new AeroFilter(Constants.EQUAL_VALUE, Constants.LABEL_COL_NAME,
                        fields.get(Constants.LABEL_COL_NAME)));

        filters.add(
                new AeroFilter(Constants.EQUAL_VALUE, Constants.PROPERTY_KEY_COL_NAME,
                        fields.get(Constants.PROPERTY_KEY_COL_NAME)));

        filters.add(
                new AeroFilter(Constants.EQUAL_VALUE, Constants.PROPERTY_VALUE_COL_NAME,
                        fields.get(Constants.PROPERTY_VALUE_COL_NAME)));

        return connect
                /*
                 * Aerospike read query with three filter
                 * conditions combined with `and`.
                 */
                .query(setname, new AeroFilters("and", filters, -1));

    }
}
