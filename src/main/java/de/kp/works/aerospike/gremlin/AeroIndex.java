package de.kp.works.aerospike.gremlin;
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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import de.kp.works.aerospike.AeroConnect;

/**
 * This class is responsible for building secondary
 * indices on Aerospike bins
 */
public class AeroIndex {

    private final AerospikeClient client;
    private final WritePolicy policy;

    private final String namespace;
    private final String setname;

    public AeroIndex(AeroConnect connect) {

        this.client = connect.getClient();
        this.policy = connect.getWritePolicy();

        this.namespace = connect.namespace();
        this.setname = connect.setname();
    }

    public void edgeIndices() {

        String _setname = setname +  "_" + Constants.EDGES;
        /*
         * 1 : Constants.ID_COL_NAME (String)
         * 2 : Constants.ID_TYPE_COL_NAME (String)
         * 3 : Constants.LABEL_COL_NAME (String)
         * 4 : Constants.TO_COL_NAME (String)
         * 5 : Constants.TO_TYPE_COL_NAME (String)
         * 6 : Constants.FROM_COL_NAME (String)
         * 7 : Constants.FROM_TYPE_COL_NAME (String)
         * 8 : Constants.CREATED_AT_COL_NAME (Long)
         * 9 : Constants.UPDATED_AT_COL_NAME (Long)
         * 10: Constants.PROPERTY_KEY_COL_NAME (String)
         * 11: Constants.PROPERTY_TYPE_COL_NAME (String)
         * 12: Constants.PROPERTY_VALUE_COL_NAME (String)
         */
        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.ID_COL_NAME,
                Constants.ID_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.LABEL_COL_NAME,
                Constants.LABEL_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.TO_COL_NAME,
                Constants.TO_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.FROM_COL_NAME,
                Constants.FROM_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.PROPERTY_KEY_COL_NAME,
                Constants.PROPERTY_KEY_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.PROPERTY_VALUE_COL_NAME,
                Constants.PROPERTY_VALUE_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

    }

    public void vertexIndices() {

        String _setname = setname +  "_" + Constants.VERTICES;
        /*
         * 1 : Constants.ID_COL_NAME (String)
         * 2 : Constants.ID_TYPE_COL_NAME (String)
         * 3 : Constants.LABEL_COL_NAME (String)
         * 4 : Constants.CREATED_AT_COL_NAME (Long)
         * 5 : Constants.UPDATED_AT_COL_NAME (Long)
         * 6 : Constants.PROPERTY_KEY_COL_NAME (String)
         * 7 : Constants.PROPERTY_TYPE_COL_NAME (String)
         * 8 : Constants.PROPERTY_VALUE_COL_NAME (String)
         */
        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.ID_COL_NAME,
                Constants.ID_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.LABEL_COL_NAME,
                Constants.LABEL_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.PROPERTY_KEY_COL_NAME,
                Constants.PROPERTY_KEY_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

        client.createIndex(
                policy,
                namespace,
                _setname,
                "idx_" + _setname + "_" + Constants.PROPERTY_VALUE_COL_NAME,
                Constants.PROPERTY_VALUE_COL_NAME,
                IndexType.STRING
        ).waitTillComplete();

    }
}
