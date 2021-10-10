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
import de.kp.works.aerospikegraph.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class AeroLabelQuery extends AeroQuery {
    /**
     * Retrieve all elements that refer to the provided
     * label, either edges or vertices.
     */
    public AeroLabelQuery(String name, AeroConnect connect, String label) {
        super(name, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();
        fields.put(Constants.LABEL_COL_NAME, label);

    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        /*
         * The query logic demands for records that have
         * label value equal to the specified one.
         */
        List<AeroFilter> filters = new ArrayList<>();
        filters.add(
                new AeroFilter(Constants.EQUAL_VALUE, Constants.LABEL_COL_NAME,
                        fields.get(Constants.LABEL_COL_NAME)));

        return connect
                /*
                 * Aerospike read query with a single
                 * filter condition
                 */
                .query(setname, new AeroFilters("and", filters));

    }

}
