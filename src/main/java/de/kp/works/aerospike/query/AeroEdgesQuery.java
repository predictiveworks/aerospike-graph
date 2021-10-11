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

import com.aerospike.client.Record;
import com.google.common.collect.Streams;
import de.kp.works.aerospike.AeroConnect;
import de.kp.works.aerospike.AeroFilter;
import de.kp.works.aerospike.AeroFilters;
import de.kp.works.aerospike.KeyRecord;
import de.kp.works.aerospikegraph.Constants;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.*;
import java.util.stream.Stream;

public class AeroEdgesQuery extends AeroQuery {
    /*
     * An indicator to determine which query use
     * case must be supported.
     */
    private final String queryType;

    public AeroEdgesQuery(String name, AeroConnect connect,
                          Object vertex, Direction direction, String... labels) {
        super(name, connect);
        /*
         * Transform the provided properties into fields;
         * this results in a request where either from or
         * to (depends on direction) is specified and a
         * set of labels.
         */
        fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(Constants.LABEL_COL_NAME, String.join(",", labels));
        /*
         * This use case is described as `withLabels`, while
         * the second use case is specified as `withLabel`.
         */
        queryType = "withLabels";
    }

    public AeroEdgesQuery(String cacheName, AeroConnect connect,
                          Object vertex, Direction direction, String label, String key, Object value) {
        super(cacheName, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(Constants.LABEL_COL_NAME, label);

        fields.put(Constants.PROPERTY_KEY_COL_NAME, key);
        fields.put(Constants.PROPERTY_VALUE_COL_NAME, value.toString());
        /*
         * This use case is described as `withLabel`, while
         * the first use case is specified as `withLabels`.
         */
        queryType = "withLabel";
    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        /*
         * Edge direction is used as the leading filter condition
         * for the Aerospike query; additional filters are all
         * evaluated on the client side.
         */
        List<AeroFilter> filters = new ArrayList<>();
        if (fields.containsKey(Constants.TO_COL_NAME)) {
            filters.add(
                    new AeroFilter(Constants.EQUAL_VALUE, Constants.TO_COL_NAME,
                            fields.get(Constants.TO_COL_NAME)));

        }
        else {
            filters.add(
                    new AeroFilter(Constants.EQUAL_VALUE, Constants.FROM_COL_NAME,
                            fields.get(Constants.FROM_COL_NAME)));

        }
        /*
         * Distinguish between the supported use cases
         */
        if (queryType.equals("withLabels")) {
            /*
             * No further filter conditions can be assigned; the check
             * whether the respective labels are contained, must be
             * performed here.
             */
            Stream<String> labels =  Arrays.stream(fields.get(Constants.LABEL_COL_NAME).split(","));
            /*
             * Retrieve query result from Aerospike backend and
             * prepare for further filter processing.
             */
            Iterator<KeyRecord> keyRecords = connect
                    .query(setname, new AeroFilters("and", filters, -1));

            return Streams.stream(keyRecords).filter(keyRecord -> {

                Record record = keyRecord.record();
                String label = record.getString(Constants.LABEL_COL_NAME);

                return labels.anyMatch(l -> l.equals(label));

            }).iterator();

        }
        else if (queryType.equals("withLabel")) {
            /*
             * Assign label, property key and value as additional
             * filter conditions. Note, these conditions must be
             * evaluated on the client side, due to Aerospike's
             * restriction to a single filter condition.
             */
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
                     * Concatenate all filter conditions with
                     * an `and` statement.
                     */
                    .query(setname, new AeroFilters("and", filters, -1));

        }
        return null;
    }

}
