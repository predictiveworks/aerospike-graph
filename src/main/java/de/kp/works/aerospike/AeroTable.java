package de.kp.works.aerospike;
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

import de.kp.works.aerospike.mutate.*;
import de.kp.works.aerospike.query.*;
import de.kp.works.aerospike.gremlin.AeroVertex;
import de.kp.works.aerospike.gremlin.ElementType;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;

public class AeroTable extends AeroBaseTable {

    public AeroTable(String name, AeroConnect connect) {
        super(name, connect);
    }
    /**
     * This method adds or updates an Aerospike cache entry;
     * note, the current implementation requires a fully
     * qualified cache entry.
     */
    public boolean put(AeroPut aeroPut) throws Exception {

        if (connect == null) return false;
        if (connect.getClient() == null) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                putEdge(aeroPut);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                putVertex(aeroPut);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

            return true;

        } catch (Exception e) {
            return false;
        }

    }
    /**
     * Delete supports deletion of an entire element
     * (edge or vertex) and also the removal of a certain
     * property.
     */
    public boolean delete(AeroDelete aeroDelete) throws Exception {

        if (connect == null) return false;
        if (connect.getClient() == null) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                deleteEdge(aeroDelete);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                deleteVertex(aeroDelete);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

            return true;

        } catch (Exception e) {
            return false;
        }
    }

    public Object increment(AeroIncrement aeroIncrement) {

        if (connect == null) return false;
        if (connect.getClient() == null) return false;
        /*
         * In case of an increment, the respective incremented
         * value is returned,
         */
        try {

            if (elementType.equals(ElementType.EDGE)) {
                return incrementEdge(aeroIncrement);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                return incrementVertex(aeroIncrement);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");


        } catch (Exception e) {
            return null;
        }

    }

    public void batch(List<AeroMutation> mutations, Object[] results) throws Exception {

        for (int i = 0; i < mutations.size(); i++) {
            /*
             * Determine the respective mutation
             */
            AeroMutation mutation = mutations.get(i);
            if (mutation.mutationType.equals(AeroMutationType.DELETE)) {

                AeroDelete deleteMutation = (AeroDelete)mutation;

                boolean success = delete(deleteMutation);
                if (!success) {
                    /*
                     * See [Mutators]: In case of a failed delete
                     * operation an exception is returned
                     */
                    results[i] = new Exception(
                            "Deletion of element '" + deleteMutation.getId().toString() + "' failed in table '" + name + "'.");

                }
                else
                    results[i] = deleteMutation.getId();

            }
            else if (mutation.mutationType.equals(AeroMutationType.INCREMENT)) {

                AeroIncrement incrementMutation = (AeroIncrement)mutation;

                Object value = increment(incrementMutation);
                if (value == null) {
                    /*
                     * See [Mutators]: In case of a failed delete
                     * operation an exception is returned
                     */
                    results[i] = new Exception(
                            "Increment of element '" + incrementMutation.getId().toString() + "' failed in table '" + name + "'.");

                }
                else
                    results[i] = value;

            }
            else {

                AeroPut putMutation = (AeroPut)mutation;

                boolean success = put(putMutation);
                if (!success) {
                    /*
                     * See [Mutators]: In case of a failed delete
                     * operation an exception is returned
                     */
                    results[i] = new Exception(
                            "Deletion of element '" + putMutation.getId().toString() + "' failed in table '" + name + "'.");

                }
                else
                    results[i] = putMutation.getId();

            }

        }
    }
    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public AeroResult[] get(List<Object> ids) {
        AeroGetQuery aeroQuery = new AeroGetQuery(name, connect, ids);

        List<AeroResult> result = aeroQuery.getResult();
        return result.toArray(new AeroResult[0]);
    }
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public AeroResult get(Object id) {
        AeroGetQuery aeroQuery = new AeroGetQuery(name, connect, id);

        List<AeroResult> result = aeroQuery.getResult();
        if (result.isEmpty()) return null;
        return result.get(0);
    }

    /**
     * Returns an [AeroQuery] to retrieve all elements
     */
    public AeroQuery getAllQuery() {
        return new AeroAllQuery(name, connect);
    }

    /**
     * Returns an [AeroQuery] to retrieve all elements
     * that are referenced by a certain label
     */
    public AeroQuery getLabelQuery(String label) {
        return new AeroLabelQuery(name, connect, label);
    }
    /**
     * Returns an [AeroQuery] to retrieve a specified
     * number of (ordered) elements from the beginning
     * of the cache
     */
    public AeroQuery getLimitQuery(int limit) {
        return new AeroLimitQuery(name, connect, limit);
    }

    public AeroQuery getLimitQuery(Object fromId, int limit) {
        return new AeroLimitQuery(name, connect, fromId, limit);
    }

    public AeroQuery getLimitQuery(String label, String key, Object inclusiveFrom, int limit) {
        return new AeroLimitQuery(name, connect, label, key, inclusiveFrom, limit);    }
    /**
     * Returns an [AeroQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value
     */
    public AeroQuery getPropertyQuery(String label, String key, Object value) {
        return new AeroPropertyQuery(name, connect, label, key, value);
    }
    /**
     * Returns an [AeroQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value range
     */
    public AeroQuery getRangeQuery(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        return new AeroRangeQuery(name, connect, label, key, inclusiveFrom, exclusiveTo);
    }

    /* EDGE READ SUPPORT */

    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public AeroQuery getEdgesQuery(AeroVertex vertex, Direction direction, String... labels) {
        return new AeroEdgesQuery(name, connect, vertex.id(), direction, labels);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public AeroQuery getEdgesQuery(AeroVertex vertex, Direction direction, String label,
                                     String key, Object value) {
        return new AeroEdgesQuery(name, connect, vertex.id(), direction, label, key, value);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values
     */
    public AeroQuery getEdgesInRangeQuery(AeroVertex vertex, Direction direction, String label,
                                            String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return new AeroEdgesInRangeQuery(name, connect, vertex.id(), direction, label,
                key, inclusiveFromValue, exclusiveToValue);
    }

}
