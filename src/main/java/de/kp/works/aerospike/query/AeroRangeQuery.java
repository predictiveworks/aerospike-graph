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

public class AeroRangeQuery extends AeroQuery {
    /**
     * Retrieves all elements that refer to specific
     * label and a range of values of a property that
     * can be sorted in ASC order.
     */
    public AeroRangeQuery(String name, AeroConnect connect,
                          String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        super(name, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();

        fields.put(Constants.LABEL_COL_NAME, label);
        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);

        fields.put(Constants.INCLUSIVE_FROM_VALUE, inclusiveFromValue.toString());
        fields.put(Constants.EXCLUSIVE_TO_VALUE, exclusiveToValue.toString());

    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        List<AeroFilter> filters = new ArrayList<>();
        /*
         * The leading filter is the label filter
         */
        filters.add(
                new AeroFilter(Constants.EQUAL_VALUE, Constants.LABEL_COL_NAME,
                        fields.get(Constants.LABEL_COL_NAME)));
        /*
         * The second filter is on the provided property;
         * note, it is expected that the property values
         * are defined as numbers
         */
        filters.add(
                new AeroFilter(Constants.EQUAL_VALUE, Constants.PROPERTY_KEY_COL_NAME,
                        fields.get(Constants.PROPERTY_KEY_COL_NAME)));

        filters.add(
                new AeroFilter(Constants.INCLUSIVE_FROM_VALUE, Constants.PROPERTY_VALUE_COL_NAME,
                        fields.get(Constants.INCLUSIVE_FROM_VALUE)));

        filters.add(
                new AeroFilter(Constants.EXCLUSIVE_TO_VALUE, Constants.PROPERTY_VALUE_COL_NAME,
                        fields.get(Constants.EXCLUSIVE_TO_VALUE)));

        return connect
                .query(setname, new AeroFilters("and", filters, -1));

    }
}
