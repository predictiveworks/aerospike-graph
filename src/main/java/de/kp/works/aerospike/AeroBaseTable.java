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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import de.kp.works.aerospike.mutate.AeroDelete;
import de.kp.works.aerospike.mutate.AeroIncrement;
import de.kp.works.aerospike.mutate.AeroPut;
import de.kp.works.aerospike.query.AeroEdgeQuery;
import de.kp.works.aerospike.query.AeroEdgesExistQuery;
import de.kp.works.aerospike.query.AeroGetQuery;
import de.kp.works.aerospike.gremlin.Constants;
import de.kp.works.aerospike.gremlin.ElementType;
import de.kp.works.aerospike.gremlin.ValueType;

import java.util.*;
import java.util.stream.Collectors;

public class AeroBaseTable {

    protected final String name;
    protected final AeroConnect connect;

    protected final ElementType elementType;

    public AeroBaseTable(String name, AeroConnect connect) {

        this.name = name;
        this.connect = connect;

        if (name.equals(connect.setname() + "_" + Constants.EDGES)) {
            elementType = ElementType.EDGE;
        }
        else if (name.equals(connect.setname() + "_" + Constants.VERTICES)) {
            elementType = ElementType.VERTEX;
        }
        else
            elementType = ElementType.UNDEFINED;

    }

    /** METHODS TO SUPPORT BASIC CRUD OPERATIONS **/

    protected Object incrementEdge(AeroIncrement aeroIncrement) {
       Object edgeId = aeroIncrement.getId();
        List<AeroEdgeEntry> edge = getEdge(edgeId);
        /*
         * We expect that the respective edge exists;
         * returning `null` leads to an exception that
         * is moved to the user interface
         */
        if (edge.isEmpty()) return null;
        return incrementEdge(aeroIncrement, edge);
    }

    protected Object incrementVertex(AeroIncrement aeroIncrement) {
        Object vertexId = aeroIncrement.getId();
        List<AeroVertexEntry> vertex = getVertex(vertexId);
        /*
         * We expect that the respective vertex exists;
         * returning `null` leads to an exception that
         * is moved to the user interface
         */
        if (vertex.isEmpty()) return null;
        return incrementVertex(aeroIncrement, vertex);
    }

    protected void putEdge(AeroPut aeroPut) throws Exception {
        /*
         * STEP #1: Retrieve existing edge entries
         * that refer to the provided id
         */
        Object edgeId = aeroPut.getId();
        List<AeroEdgeEntry> edge = getEdge(edgeId);

        if (edge.isEmpty())
            createEdge(aeroPut);

        else
            updateEdge(aeroPut, edge);

    }

    protected void putVertex(AeroPut aeroPut) throws Exception {
        Object vertexId = aeroPut.getId();
        List<AeroVertexEntry> vertex = getVertex(vertexId);

        if (vertex.isEmpty())
            createVertex(aeroPut);

        else
            updateVertex(aeroPut, vertex);
    }

    /**
     * The current version of [AeroGraph] supports two different
     * approaches to retrieve an instance of an [Edge]:
     *
     * Either by identifier or by `from` and `to` fields
     */
    protected void deleteEdge(AeroDelete aeroDelete) throws Exception {

        List<AeroEdgeEntry> edge;
        Object edgeId = aeroDelete.getId();
        if (edgeId != null) {
            /*
             * This is the default approach to retrieve the
             * instance of an [Edge]
             */
            edge = getEdge(edgeId);
        }
        else {
            /*
             * Retrieve `from` and `to` columns
             */
            AeroColumn fromColumn = aeroDelete.getColumn(Constants.FROM_COL_NAME);
            AeroColumn toColumn = aeroDelete.getColumn(Constants.TO_COL_NAME);

            if (fromColumn == null || toColumn == null)
                throw new Exception("At least one of the columns `from` and `or` are missing.");

            Object fromId = fromColumn.getColValue();
            Object toId = toColumn.getColValue();

            edge = getEdge(fromId, toId);

        }

        if (!edge.isEmpty()) deleteEdge(aeroDelete, edge);
    }

    protected void deleteVertex(AeroDelete aeroDelete) throws Exception {
        Object vertexId = aeroDelete.getId();
        List<AeroVertexEntry> vertex = getVertex(vertexId);

        if (!vertex.isEmpty()) deleteVertex(aeroDelete, vertex);
    }
    /**
     * Create, update & delete operation for edges
     * are manipulating operations of the respective
     * cache entries.
     *
     * Reminder: A certain edge is defined as a list
     * of (edge) cache entries
     */
    protected List<AeroEdgeEntry> getEdge(Object id) {
        AeroGetQuery query = new AeroGetQuery(name, connect, id);
        return query.getEdgeEntries();
    }

    protected List<AeroEdgeEntry> getEdge(Object fromId, Object toId) {
        AeroEdgeQuery query = new AeroEdgeQuery(name, connect, fromId, toId);
        return query.getEdgeEntries();
    }
    /**
     * Create, update & delete operation for vertices
     * are manipulating operations of the respective
     * cache entries.
     *
     * Reminder: A certain vertex is defined as a list
     * of (vertex) cache entries
     */
    protected List<AeroVertexEntry> getVertex(Object id) {
        AeroGetQuery query = new AeroGetQuery(name, connect, id);
        return query.getVertexEntries();
    }
    /**
     * Check whether a vertex is referenced by edges
     * either as `from` or `to` vertex
     */
    protected boolean hasEdges(Object vertex) {
        AeroEdgesExistQuery query = new AeroEdgesExistQuery(name, connect, vertex);
        List<AeroEdgeEntry> edges = query.getEdgeEntries();

        return !edges.isEmpty();
    }
    /**
     * The provided [AeroPut] is transformed into a list of
     * [AeroEdgeEntry] and these entries are put into cache
     */
    protected void createEdge(AeroPut aeroPut) throws Exception {

        String id         = null;
        String idType     = null;
        String label      = null;
        String toId       = null;
        String toIdType   = null;
        String fromId     = null;
        String fromIdType = null;

        Long createdAt = System.currentTimeMillis();
        Long updatedAt = System.currentTimeMillis();

        List<AeroEdgeEntry> entries = new ArrayList<>();
        /*
         * STEP #1: Move through all columns and
         * determine the common fields of all entries
         */
        for (AeroColumn column : aeroPut.getColumns()) {
            switch (column.getColName()) {
                case Constants.ID_COL_NAME: {
                    id = column.getColValue().toString();
                    idType = column.getColType();
                    break;
                }
                case Constants.LABEL_COL_NAME: {
                    label = column.getColValue().toString();
                    break;
                }
                case Constants.TO_COL_NAME: {
                    toId = column.getColValue().toString();
                    toIdType = column.getColType();
                    break;
                }
                case Constants.FROM_COL_NAME: {
                    fromId = column.getColValue().toString();
                    fromIdType = column.getColType();
                    break;
                }
                case Constants.CREATED_AT_COL_NAME: {
                    createdAt = (Long)column.getColValue();
                    break;
                }
                case Constants.UPDATED_AT_COL_NAME: {
                    updatedAt = (Long)column.getColValue();
                    break;
                }
                default:
                    break;
            }
        }
        /*
         * Check whether the core fields of an edge entry
         * are provided
         */
        if (id == null || idType == null || label == null || toId == null || toIdType == null || fromId == null || fromIdType == null)
            throw new Exception("Number of parameters provided is not sufficient to create an edge.");

        /*
         * STEP #2: Move through all property columns
         */
        for (AeroColumn column : aeroPut.getColumns()) {
            switch (column.getColName()) {
                case Constants.ID_COL_NAME:
                case Constants.LABEL_COL_NAME:
                case Constants.TO_COL_NAME:
                case Constants.FROM_COL_NAME:
                case Constants.CREATED_AT_COL_NAME:
                case Constants.UPDATED_AT_COL_NAME: {
                    break;
                }
                default: {
                    /*
                     * Build an entry for each property
                     */
                    String propKey   = column.getColName();
                    String propType  = column.getColType();
                    String propValue = column.getColValue().toString();
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();

                    entries.add(new AeroEdgeEntry(
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
                            propValue));

                    break;
                }
            }
        }
        /*
         * STEP #3: Check whether the entries are still empty,
         * i.e. a vertex without properties will be created
         */
        if (entries.isEmpty()) {
            String emptyValue = "*";
            /*
             * For a create request, we must generate
             * a unique cache key for each entry
             */
            String cacheKey = UUID.randomUUID().toString();
            entries.add(new AeroEdgeEntry(cacheKey,
                    id, idType, label, toId, toIdType, fromId, fromIdType,
                    createdAt, updatedAt, emptyValue, emptyValue, emptyValue));

        }
        /*
         * STEP #4: Persist all entries that describe the edge
         * to the Aerospike edge cache
         */
        writeEdge(entries);
    }

    /**
     * This method supports the modification of an existing
     * edge; this implies the update of existing property
     * values as well as the creation of new properties
     *
     * TODO ::
     * The current implementation does not support any
     * transactions to ensure consistency.
     */
    protected void updateEdge(AeroPut aeroPut, List<AeroEdgeEntry> edge) {
        /*
         * STEP #1: Retrieve all properties that are
         * provided
         */
        List<AeroColumn> properties = aeroPut.getProperties()
                .collect(Collectors.toList());
        /*
         * STEP #2: Distinguish between those properties
         * that are edge properties already and determine
         * those that do not exist
         */
        List<AeroColumn> knownProps = new ArrayList<>();
        List<AeroColumn> unknownProps = new ArrayList<>();

        for (AeroColumn property : properties) {
            String propKey = property.getColName();
            if (edge.stream().anyMatch(entry -> entry.propKey.equals(propKey)))
                knownProps.add(property);

            else
                unknownProps.add(property);
        }
        /*
         * STEP #3: Update known properties
         */
        List<AeroEdgeEntry> updatedEntries = edge.stream()
                .map(entry -> {
                    String propKey = entry.propKey;
                    /*
                     * Determine provided values that matches
                     * the property key of the entry
                     */
                    AeroColumn property = knownProps.stream()
                            .filter(p -> p.getColName().equals(propKey)).collect(Collectors.toList()).get(0);

                    Object newValue = property.getColValue();
                    return new AeroEdgeEntry(
                            entry.cacheKey,
                            entry.id,
                            entry.idType,
                            entry.label,
                            entry.toId,
                            entry.toIdType,
                            entry.fromId,
                            entry.fromIdType,
                            entry.createdAt,
                            /*
                             * Update the entry's `updatedAt` timestamp
                             */
                            System.currentTimeMillis(),
                            entry.propKey,
                            entry.propType,
                            newValue.toString());

                })
                .collect(Collectors.toList());

        writeEdge(updatedEntries);
        /*
         * STEP #4: Add unknown properties; note, we use the first
         * edge entry as a template for the common parameters
         */
        AeroEdgeEntry template = edge.get(0);
        List<AeroEdgeEntry> newEntries = unknownProps.stream()
                .map(property -> {
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();
                    return new AeroEdgeEntry(
                            cacheKey,
                            template.id,
                            template.idType,
                            template.label,
                            template.toId,
                            template.toIdType,
                            template.fromId,
                            template.fromIdType,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            property.getColName(),
                            property.getColType(),
                            property.getColValue().toString());
                })
                .collect(Collectors.toList());

        writeEdge(newEntries);
    }
    /**
     * This method supports the deletion of an entire edge
     * or just specific properties of an existing edge.
     */
    protected void deleteEdge(AeroDelete aeroDelete, List<AeroEdgeEntry> edge) {

        List<Key> cacheKeys;

        List<AeroColumn> columns = aeroDelete.getColumns();
        /*
         * STEP #1: Check whether we must delete the
         * entire edge or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * edge must be deleted.
             */
            cacheKeys = edge.stream()
                    .map(entry -> connect.getKey(entry.cacheKey, ElementType.EDGE))
                    .collect(Collectors.toList());
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
            List<String> propKeys = aeroDelete.getProperties()
                    .map(c -> c.getColValue().toString())
                    .collect(Collectors.toList());

            cacheKeys = edge.stream()
                    /*
                     * Restrict to those cache entries that refer
                     * to the provided property keys
                     */
                    .filter(entry -> propKeys.contains(entry.propKey))
                    .map(entry -> connect.getKey(entry.cacheKey, ElementType.EDGE))
                    .collect(Collectors.toList());

        }

        if (!cacheKeys.isEmpty())
            connect.removeAll(cacheKeys);
    }
    /**
     * This method increments a certain property value
     */
    protected Object incrementEdge(AeroIncrement aeroIncrement, List<AeroEdgeEntry> edge) {
        AeroColumn column = aeroIncrement.getColumn();
        if (column == null) return null;
        /*
         * Check whether the column value is a [Long]
         */
        String colType = column.getColType();
        if (!colType.equals(ValueType.LONG.name()))
            return null;
        /*
         * Restrict to that edge entry that refer to the
         * provided column
         */
        String colName = column.getColName();
        String colValue = column.getColValue().toString();

        List<AeroEdgeEntry> entries = edge.stream()
                .filter(entry -> entry.propKey.equals(colName) && entry.propType.equals(colType) && entry.propValue.equals(colValue))
                .collect(Collectors.toList());

        if (entries.isEmpty()) return null;
        AeroEdgeEntry entry = entries.get(0);

        long oldValue = Long.parseLong(entry.propValue);
        Long newValue = oldValue + 1;

        AeroEdgeEntry newEntry = new AeroEdgeEntry(
                entry.cacheKey,
                entry.id,
                entry.idType,
                entry.label,
                entry.toId,
                entry.toIdType,
                entry.fromId,
                entry.fromIdType,
                entry.createdAt,
                System.currentTimeMillis(),
                entry.propKey,
                entry.propType,
                newValue.toString());

        writeEdge(Collections.singletonList(newEntry));
        return newValue;
    }
    /**
     * The provided [AeroPut] is transformed into a list of
     * [AeroVertexEntry] and these entries are put into cache
     */
    protected void createVertex(AeroPut aeroPut) throws Exception {

        String id = null;
        String idType = null;
        String label = null;
        Long createdAt = System.currentTimeMillis();
        Long updatedAt = System.currentTimeMillis();

        List<AeroVertexEntry> entries = new ArrayList<>();
        /*
         * STEP #1: Move through all columns and
         * determine the common fields of all entries
         */
        for (AeroColumn column : aeroPut.getColumns()) {
            switch (column.getColName()) {
                case Constants.ID_COL_NAME: {
                    id = column.getColValue().toString();
                    idType = column.getColType();
                    break;
                }
                case Constants.LABEL_COL_NAME: {
                    label = column.getColValue().toString();
                    break;
                }
                case Constants.CREATED_AT_COL_NAME: {
                    createdAt = (Long)column.getColValue();
                    break;
                }
                case Constants.UPDATED_AT_COL_NAME: {
                    updatedAt = (Long)column.getColValue();
                    break;
                }
                default:
                    break;
            }
        }
        /*
         * Check whether the core fields of a vertex entry
         * are provided
         */
        if (id == null || idType == null || label == null)
            throw new Exception("Number of parameters provided is not sufficient to create a vertex.");
        /*
         * STEP #2: Move through all property columns
         */
        for (AeroColumn column : aeroPut.getColumns()) {
            switch (column.getColName()) {
                case Constants.ID_COL_NAME:
                case Constants.LABEL_COL_NAME:
                case Constants.CREATED_AT_COL_NAME:
                case Constants.UPDATED_AT_COL_NAME: {
                    break;
                }
                default: {
                    /*
                     * Build an entry for each property
                     */
                    String propKey   = column.getColName();
                    String propType  = column.getColType();
                    String propValue = column.getColValue().toString();
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();

                    entries.add(new AeroVertexEntry(
                            cacheKey,
                            id,
                            idType,
                            label,
                            createdAt,
                            updatedAt,
                            propKey,
                            propType,
                            propValue));

                    break;
                }
            }
        }
        /*
         * STEP #3: Check whether the entries are still empty,
         * i.e. a vertex without properties will be created
         */
        if (entries.isEmpty()) {
            String emptyValue = "*";
            /*
             * For a create request, we must generate
             * a unique cache key for each entry
             */
            String cacheKey = UUID.randomUUID().toString();
            entries.add(new AeroVertexEntry(cacheKey,
                    id, idType, label, createdAt, updatedAt, emptyValue, emptyValue, emptyValue));

        }
        /*
         * STEP #4: Persist all entries that describe the vertex
         * to the Aerospike vertex cache
         */
        writeVertex(entries);

    }
    /**
     * This method supports the modification of an existing
     * vertex; this implies the update of existing property
     * values as well as the creation of new properties
     *
     * TODO ::
     * The current implementation does not support any
     * transactions to ensure consistency.
     */
    protected void updateVertex(AeroPut aeroPut, List<AeroVertexEntry> vertex) {
        /*
         * STEP #1: Retrieve all properties that are
         * provided
         */
        List<AeroColumn> properties = aeroPut.getProperties()
                .collect(Collectors.toList());
        /*
         * STEP #2: Distinguish between those properties
         * that are edge properties already and determine
         * those that do not exist
         */
        List<AeroColumn> knownProps = new ArrayList<>();
        List<AeroColumn> unknownProps = new ArrayList<>();

        for (AeroColumn property : properties) {
            String propKey = property.getColName();
            if (vertex.stream().anyMatch(entry -> entry.propKey.equals(propKey)))
                knownProps.add(property);

            else
                unknownProps.add(property);
        }
        /*
         * STEP #3: Update known properties
         */
        List<AeroVertexEntry> updatedEntries = vertex.stream()
                .map(entry -> {
                    String propKey = entry.propKey;
                    /*
                     * Determine provided values that matches
                     * the property key of the entry
                     */
                    AeroColumn property = knownProps.stream()
                            .filter(p -> p.getColName().equals(propKey)).collect(Collectors.toList()).get(0);

                    Object newValue = property.getColValue();
                    return new AeroVertexEntry(
                            entry.cacheKey,
                            entry.id,
                            entry.idType,
                            entry.label,
                            entry.createdAt,
                            /*
                             * Update the entry's `updatedAt` timestamp
                             */
                            System.currentTimeMillis(),
                            entry.propKey,
                            entry.propType,
                            newValue.toString());

                })
                .collect(Collectors.toList());

        writeVertex(updatedEntries);
        /*
         * STEP #4: Add unknown properties; note, we use the first
         * vertex entry as a template for the common parameters
         */
        AeroVertexEntry template = vertex.get(0);
        List<AeroVertexEntry> newEntries = unknownProps.stream()
                .map(property -> {
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();
                    return new AeroVertexEntry(
                            cacheKey,
                            template.id,
                            template.idType,
                            template.label,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            property.getColName(),
                            property.getColType(),
                            property.getColValue().toString());
                })
                .collect(Collectors.toList());

        writeVertex(newEntries);
    }
    /**
     * This method supports the deletion of an entire vertex
     * or just specific properties of an existing vertex.
     *
     * When and entire vertex must be deleted, this methods
     * also checks whether the vertex is referenced by an edge
     */
    protected void deleteVertex(AeroDelete aeroDelete, List<AeroVertexEntry> vertex) throws Exception {

        List<Key> keys;

        List<AeroColumn> columns = aeroDelete.getColumns();
        /*
         * STEP #1: Check whether we must delete the
         * entire vertex or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * vertex must be deleted.
             */
            Object id = aeroDelete.getId();
            if (hasEdges(id))
                throw new Exception("The vertex '" + id.toString() + "' is referenced by at least one edge.");

            keys = vertex.stream()
                    .map(entry -> connect.getKey(entry.cacheKey, ElementType.VERTEX))
                    .collect(Collectors.toList());
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
            List<String> propKeys = aeroDelete.getProperties()
                    .map(AeroColumn::getColName)
                    .collect(Collectors.toList());

            keys = vertex.stream()
                    /*
                     * Restrict to those cache entries that refer
                     * to the provided property keys
                     */
                    .filter(entry -> propKeys.contains(entry.propKey))
                    .map(entry -> connect.getKey(entry.cacheKey, ElementType.VERTEX)).collect(Collectors.toList());

        }

        connect.removeAll(keys);
    }

    protected Object incrementVertex(AeroIncrement aeroIncrement, List<AeroVertexEntry> vertex) {
        AeroColumn column = aeroIncrement.getColumn();
        if (column == null) return null;
        /*
         * Check whether the column value is a [Long]
         */
        String colType = column.getColType();
        if (!colType.equals(ValueType.LONG.name()))
            return null;
        /*
         * Restrict to that vertex entry that refer to the
         * provided column
         */
        String colName = column.getColName();
        String colValue = column.getColValue().toString();

        List<AeroVertexEntry> entries = vertex.stream()
                .filter(entry -> entry.propKey.equals(colName) && entry.propType.equals(colType) && entry.propValue.equals(colValue))
                .collect(Collectors.toList());

        if (entries.isEmpty()) return null;
        AeroVertexEntry entry = entries.get(0);

        long oldValue = Long.parseLong(entry.propValue);
        Long newValue = oldValue + 1;

        AeroVertexEntry newEntry = new AeroVertexEntry(
                entry.cacheKey,
                entry.id,
                entry.idType,
                entry.label,
                entry.createdAt,
                System.currentTimeMillis(),
                entry.propKey,
                entry.propType,
                newValue.toString());

        writeVertex(Collections.singletonList(newEntry));
        return newValue;
    }

    /**
     * Supports create and update operations for edges
     */
    private void writeEdge(List<AeroEdgeEntry> entries) {
        /*
         * An edge represents a list of edge entries
         */
        Map<Key, List<Bin>> row = new HashMap<>();
        for (AeroEdgeEntry entry : entries) {

            List<Bin> bins = new ArrayList<>();

            bins.add(new Bin(Constants.ID_COL_NAME,      entry.id));
            bins.add(new Bin(Constants.ID_TYPE_COL_NAME, entry.idType));
            bins.add(new Bin(Constants.LABEL_COL_NAME,   entry.label));

            bins.add(new Bin(Constants.TO_COL_NAME,      entry.toId));
            bins.add(new Bin(Constants.TO_TYPE_COL_NAME, entry.toIdType));

            bins.add(new Bin(Constants.FROM_COL_NAME,      entry.fromId));
            bins.add(new Bin(Constants.FROM_TYPE_COL_NAME, entry.fromIdType));

            bins.add(new Bin(Constants.CREATED_AT_COL_NAME, entry.createdAt));
            bins.add(new Bin(Constants.UPDATED_AT_COL_NAME, entry.updatedAt));

            bins.add(new Bin(Constants.PROPERTY_KEY_COL_NAME,   entry.propKey));
            bins.add(new Bin(Constants.PROPERTY_TYPE_COL_NAME,  entry.propType));
            bins.add(new Bin(Constants.PROPERTY_VALUE_COL_NAME, entry.propValue));

            String uid = entry.cacheKey;
            Key key = connect.getKey(uid, ElementType.EDGE);

            row.put(key, bins);

        }

        // TODO: Start transaction

        for (Map.Entry<Key,List<Bin>> entry : row.entrySet()) {
            connect.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Supports create and update operations for vertices
     */
    private void writeVertex(List<AeroVertexEntry> entries) {
        /*
         * A  vertex represents a list of edge entries
         */
        Map<Key, List<Bin>> row = new HashMap<>();
        for (AeroVertexEntry entry : entries) {

            List<Bin> bins = new ArrayList<>();

            bins.add(new Bin(Constants.ID_COL_NAME,      entry.id));
            bins.add(new Bin(Constants.ID_TYPE_COL_NAME, entry.idType));
            bins.add(new Bin(Constants.LABEL_COL_NAME,   entry.label));

            bins.add(new Bin(Constants.CREATED_AT_COL_NAME, entry.createdAt));
            bins.add(new Bin(Constants.UPDATED_AT_COL_NAME, entry.updatedAt));

            bins.add(new Bin(Constants.PROPERTY_KEY_COL_NAME,   entry.propKey));
            bins.add(new Bin(Constants.PROPERTY_TYPE_COL_NAME,  entry.propType));
            bins.add(new Bin(Constants.PROPERTY_VALUE_COL_NAME, entry.propValue));

            String uid = entry.cacheKey;
            Key key = connect.getKey(uid, ElementType.VERTEX);

            row.put(key, bins);

        }

        // TODO: Start transaction

        for (Map.Entry<Key,List<Bin>> entry : row.entrySet()) {
            connect.put(entry.getKey(), entry.getValue());
        }

   }

}
