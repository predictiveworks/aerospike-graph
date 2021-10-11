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
import java.util.List;
import java.util.stream.Collectors;

public class AeroGetQuery extends AeroQuery {
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public AeroGetQuery(String name, AeroConnect connect, Object id) {
        super(name, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();
        fields.put(Constants.ID_COL_NAME, id.toString());

    }
    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public AeroGetQuery(String cacheName, AeroConnect connect, List<Object> ids) {
        super(cacheName, connect);
        /*
         * Transform the provided properties into fields
         */
        fields = new HashMap<>();
        fields.put(Constants.ID_COL_NAME, ids.stream()
                .map(Object::toString).collect(Collectors.joining(",")));

    }

    @Override
    protected Iterator<KeyRecord> getKeyRecords() {
        String[] userKeys = fields.get(Constants.ID_COL_NAME).split(",");
        /*
         * We use the special Aerospike read operation
         * as the ID column directly refers to the user
         * key.
         */
        return connect.getByKeys(userKeys, elementType);
    }
}
