package de.kp.works.aerospike.gremlin.models;
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

import com.google.common.collect.Streams;
import de.kp.works.aerospike.AeroTable;
import de.kp.works.aerospike.query.AeroQuery;
import de.kp.works.aerospike.gremlin.AeroGraph;
import de.kp.works.aerospike.gremlin.AeroVertex;
import de.kp.works.aerospike.gremlin.mutators.*;
import de.kp.works.aerospike.gremlin.readers.EdgeReader;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class EdgeModel extends ElementModel {

    public EdgeModel(AeroGraph graph, AeroTable table) {
        super(graph, table);
    }

    public EdgeReader getReader() {
        return new EdgeReader(graph);
    }

    /** WRITE & DELETE **/

    public void writeEdge(Edge edge) {
        Creator creator = new EdgeWriter(graph, edge);
        Mutators.create(table, creator);
    }

    public void deleteEdge(Edge edge) {
        Mutator writer = new EdgeRemover(graph, edge);
        Mutators.write(table, writer);
    }

    /** READ **/

    public Iterator<Edge> edges() {

        final EdgeReader parser = new EdgeReader(graph);
        AeroQuery aeroQuery = table.getAllQuery();

        return aeroQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    public Iterator<Edge> edges(Object fromId, int limit) {

        final EdgeReader parser = new EdgeReader(graph);
        AeroQuery igniteQuery;

        if (fromId == null)
            igniteQuery = table.getLimitQuery(limit);
        else
            igniteQuery = table.getLimitQuery(fromId, limit);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }
    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public Iterator<Edge> edges(AeroVertex vertex, Direction direction, String... labels) {

        final EdgeReader parser = new EdgeReader(graph);
        AeroQuery igniteQuery = table.getEdgesQuery(vertex, direction, labels);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public Iterator<Edge> edges(AeroVertex vertex, Direction direction, String label,
                                String key, Object value) {

        final EdgeReader parser = new EdgeReader(graph);
        AeroQuery igniteQuery = table.getEdgesQuery(vertex, direction, label, key, value);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }

    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values
     */
    public Iterator<Edge> edgesInRange(AeroVertex vertex, Direction direction, String label,
                                       String key, Object inclusiveFromValue, Object exclusiveToValue) {

        final EdgeReader parser = new EdgeReader(graph);
        AeroQuery igniteQuery = table.getEdgesInRangeQuery(vertex, direction, label, key,
                inclusiveFromValue, exclusiveToValue);

        return igniteQuery.getResult().stream()
                .map(parser::parse).iterator();
    }
    /**
     * Method to retrieve all vertices that refer to the provided
     * vertex that can be reached via related edges
     */
    public Iterator<Vertex> vertices(AeroVertex vertex, Direction direction, String... labels) {
        Iterator<Edge> edges = edges(vertex, direction, labels);
        return edgesToVertices(vertex, edges);
    }

    public Iterator<Vertex> vertices(AeroVertex vertex, Direction direction, String label,
                                     String edgeKey, Object edgeValue) {
        Iterator<Edge> edges = edges(vertex, direction, label, edgeKey, edgeValue);
        return edgesToVertices(vertex, edges);
    }

    public Iterator<Vertex> verticesInRange(AeroVertex vertex, Direction direction, String label,
                                            String edgeKey, Object inclusiveFromEdgeValue, Object exclusiveToEdgeValue) {
        Iterator<Edge> edges = edgesInRange(vertex, direction, label, edgeKey, inclusiveFromEdgeValue, exclusiveToEdgeValue);
        return edgesToVertices(vertex, edges);
    }

    private Iterator<Vertex> edgesToVertices(AeroVertex vertex, Iterator<Edge> edges) {
        /*
         * Retrieve the vertex from the respective
         * in or out vertex
         */
        List<Vertex> vertices = Streams.stream(edges).map(edge -> {

            Object inVertexId = edge.inVertex().id();
            Object outVertexId = edge.outVertex().id();

            Object vertexId = vertex.id().equals(inVertexId) ? outVertexId : inVertexId;
            return graph.findOrCreateVertex(vertexId);
        })
        .collect(Collectors.toList());

        graph.getVertexModel().load(vertices);
        return vertices.iterator();

    }

}
