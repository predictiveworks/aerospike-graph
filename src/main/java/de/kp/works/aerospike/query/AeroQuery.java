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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.google.common.collect.Streams;
import de.kp.works.aerospike.*;
import de.kp.works.aerospikegraph.Constants;
import de.kp.works.aerospikegraph.ElementType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AeroQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(AeroQuery.class);

    protected AeroConnect connect;
    protected ElementType elementType;
    /*
     * This is the operation specific setname; it is different
     * from the base setname by an additional postfix
     */
    protected String setname;
    protected HashMap<String, String> fields;

    public AeroQuery(String name, AeroConnect connect) {

        this.connect = connect;
        this.setname = name;

        if (name.equals(connect.setname() + "_" + Constants.EDGES)) {
            elementType = ElementType.EDGE;
        }
        else if (name.equals(connect.setname() + "_" + Constants.VERTICES)) {
            elementType = ElementType.VERTEX;
        }
        else
            elementType = ElementType.UNDEFINED;

    }

    protected void vertexToFields(Object vertex, Direction direction, HashMap<String, String> fields) {
        /*
         * An Edge links two Vertex objects. The Direction determines
         * which Vertex is the tail Vertex (out Vertex) and which Vertex
         * is the head Vertex (in Vertex).
         *
         * [HEAD VERTEX | OUT] -- <EDGE> --> [TAIL VERTEX | IN]
         *
         * The illustration is taken from the Apache TinkerPop [Edge]
         * documentation.
         *
         * This implies: FROM = OUT & TO = IN
         */
        if (direction.equals(Direction.IN))
            fields.put(Constants.TO_COL_NAME, vertex.toString());

        else
            fields.put(Constants.FROM_COL_NAME, vertex.toString());


    }

    protected abstract Iterator<KeyRecord> getKeyRecords();

    public List<AeroEdgeEntry> getEdgeEntries() {

        List<AeroEdgeEntry> entries = new ArrayList<>();
        if (!elementType.equals(ElementType.EDGE))
            return entries;

        try {
            Iterator<KeyRecord> keyRecords = getKeyRecords();
            entries = parseEdges(keyRecords);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries;
    }

    public List<AeroVertexEntry> getVertexEntries() {

        List<AeroVertexEntry> entries = new ArrayList<>();
        if (!elementType.equals(ElementType.VERTEX))
            return entries;

        try {
            Iterator<KeyRecord> keyRecords = getKeyRecords();
            entries = parseVertices(keyRecords);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries;
    }


    /**
     * This method returns the result of the Aerospike query
     * in form of a row-based result list. It supports user
     * specific read requests
     */
    public List<AeroResult> getResult() {

        List<AeroResult> result = new ArrayList<>();

        try {

            Iterator<KeyRecord> keyRecords = getKeyRecords();
            if (elementType.equals(ElementType.EDGE)) {
                /*
                 * Parse result and extract edge specific entries
                 */
                List<AeroEdgeEntry> entries = parseEdges(keyRecords);
                /*
                 * Group edge entries into edge rows
                 */
                return AeroTransform
                        .transformEdgeEntries(entries);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                /*
                 * Parse result and extract Vertex specific entries
                 */
                List<AeroVertexEntry> entries = parseVertices(keyRecords);
                /*
                 * Group vertex entries into edge rows
                 */
                return AeroTransform
                        .transformVertexEntries(entries);

            }
            else
                throw new Exception("Element type '" + elementType +  "' is not supported.");

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);
        }
        return result;

    }

    private List<AeroEdgeEntry> parseEdges(Iterator<KeyRecord> keyRecords) {
        /*
         * 0 : User key
         *
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
        return Streams.stream(keyRecords).map(keyRecord -> {

            Key key = keyRecord.key();
            Record record = keyRecord.record();

            String cacheKey = key.userKey.toString();

            String id     = record.getString(Constants.ID_COL_NAME);
            String idType = record.getString(Constants.ID_TYPE_COL_NAME);
            String label  = record.getString(Constants.LABEL_COL_NAME);

            String toId     = record.getString(Constants.TO_COL_NAME);
            String toIdType = record.getString(Constants.TO_TYPE_COL_NAME);

            String fromId     = record.getString(Constants.FROM_COL_NAME);
            String fromIdType = record.getString(Constants.FROM_TYPE_COL_NAME);

            Long createdAt  = record.getLong(Constants.CREATED_AT_COL_NAME);
            Long updatedAt  = record.getLong(Constants.UPDATED_AT_COL_NAME);

            String propKey   = record.getString(Constants.PROPERTY_KEY_COL_NAME);
            String propType  = record.getString(Constants.PROPERTY_TYPE_COL_NAME);
            String propValue = record.getString(Constants.PROPERTY_VALUE_COL_NAME);

            return new AeroEdgeEntry(
                    cacheKey,
                    id,
                    idType,
                    label,
                    toId,
                    toIdType,
                    fromId,
                    fromIdType,
                    createdAt,
                    updatedAt,
                    propKey,
                    propType,
                    propValue);

        }).collect(Collectors.toList());
    }

    private List<AeroVertexEntry> parseVertices(Iterator<KeyRecord> keyRecords) {
        /*
         * 0 : User key
         *
         * 1 : Constants.ID_COL_NAME (String)
         * 2 : Constants.ID_TYPE_COL_NAME (String)
         * 3 : Constants.LABEL_COL_NAME (String)
         * 4 : Constants.CREATED_AT_COL_NAME (Long)
         * 5 : Constants.UPDATED_AT_COL_NAME (Long)
         * 6 : Constants.PROPERTY_KEY_COL_NAME (String)
         * 7 : Constants.PROPERTY_TYPE_COL_NAME (String)
         * 8 : Constants.PROPERTY_VALUE_COL_NAME (String)
         */
        return Streams.stream(keyRecords).map(keyRecord -> {

            Key key = keyRecord.key();
            Record record = keyRecord.record();

            String cacheKey = key.userKey.toString();

            String id     = record.getString(Constants.ID_COL_NAME);
            String idType = record.getString(Constants.ID_TYPE_COL_NAME);
            String label  = record.getString(Constants.LABEL_COL_NAME);

            Long createdAt  = record.getLong(Constants.CREATED_AT_COL_NAME);
            Long updatedAt  = record.getLong(Constants.UPDATED_AT_COL_NAME);

            String propKey   = record.getString(Constants.PROPERTY_KEY_COL_NAME);
            String propType  = record.getString(Constants.PROPERTY_TYPE_COL_NAME);
            String propValue = record.getString(Constants.PROPERTY_VALUE_COL_NAME);

            return new AeroVertexEntry(
                    cacheKey,
                    id,
                    idType,
                    label,
                    createdAt,
                    updatedAt,
                    propKey,
                    propType,
                    propValue);

        }).collect(Collectors.toList());
    }
}
