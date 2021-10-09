package de.kp.works.aerospikegraph.models;
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

import de.kp.works.aerospike.AeroTable;
import de.kp.works.aerospike.query.AeroQuery;
import de.kp.works.aerospikegraph.AeroGraph;
import de.kp.works.aerospikegraph.mutators.*;
import de.kp.works.aerospikegraph.readers.VertexReader;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

public class VertexModel extends ElementModel {

    public VertexModel(AeroGraph graph, AeroTable table) {
        super(graph, table);
    }

    public VertexReader getReader() {
        return new VertexReader(graph);
    }

    /** WRITE & DELETE **/

    public void writeVertex(Vertex vertex) {
        Creator creator = new VertexWriter(graph, vertex);
        Mutators.create(table, creator);
    }

    public void deleteVertex(Vertex vertex) {
        Mutator writer = new VertexRemover(graph, vertex);
        Mutators.write(table, writer);
    }

    /** READ **/

    public Iterator<Vertex> vertices() {
        /*
         * The parser converts results from Aerospike
         * queries to vertices.
         */
        VertexReader parser = new VertexReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Aerospike cache.
         */
        AeroQuery aeroQuery = table.getAllQuery();

        return aeroQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    public Iterator<Vertex> vertices(Object fromId, int limit) {
        /*
         * The parser converts results from AeroEdgeEntry
         * queries to vertices.
         */
        final VertexReader parser = new VertexReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the AeroEdgeEntry cache.
         */
        AeroQuery igniteQuery;
        if (fromId == null)
            igniteQuery = table.getLimitQuery(limit);
        else
            igniteQuery = table.getLimitQuery(fromId, limit);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    /**
     * This method retrieves all vertices that refer to
     * the same label.
     */
    public Iterator<Vertex> vertices(String label) {
        /*
         * The parser converts results from AeroEdgeEntry
         * queries to vertices.
         */
        VertexReader parser = new VertexReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the AeroEdgeEntry cache.
         */
        AeroQuery igniteQuery = table.getLabelQuery(label);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    /**
     * This method retrieves all vertices that refer
     * to a certain label, property key and value
     */
    public Iterator<Vertex> vertices(String label, String key, Object value) {
        ElementHelper.validateProperty(key, value);
        /*
         * The parser converts results from AeroEdgeEntry
         * queries to vertices.
         */
        VertexReader parser = new VertexReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the AeroEdgeEntry cache.
         */
        AeroQuery igniteQuery = table.getPropertyQuery(label, key, value);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFrom, Object exclusiveTo) {

        ElementHelper.validateProperty(key, inclusiveFrom);
        ElementHelper.validateProperty(key, exclusiveTo);
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = new VertexReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        AeroQuery igniteQuery = table.getRangeQuery(label, key, inclusiveFrom, exclusiveTo);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object from, int limit, boolean reversed) {

        ElementHelper.validateProperty(key, from != null ? from : new Object());
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = new VertexReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        AeroQuery igniteQuery = table.getLimitQuery(label, key, from, limit, reversed);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }
}
