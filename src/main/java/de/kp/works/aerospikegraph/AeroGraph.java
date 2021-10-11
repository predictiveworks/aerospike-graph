package de.kp.works.aerospikegraph;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import de.kp.works.aerospike.AeroConnect;
import de.kp.works.aerospike.AeroTable;
import de.kp.works.aerospikegraph.exception.GraphException;
import de.kp.works.aerospikegraph.models.EdgeModel;
import de.kp.works.aerospikegraph.models.VertexModel;
import de.kp.works.aerospikegraph.process.strategy.optimization.AeroGraphStepStrategy;
import de.kp.works.aerospikegraph.process.strategy.optimization.AeroVertexStepStrategy;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AeroGraph implements Graph {

    private static final Logger LOGGER = LoggerFactory.getLogger(AeroGraph.class);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(AeroGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        AeroVertexStepStrategy.instance(),
                        AeroGraphStepStrategy.instance()
                ));
    }

    private final AeroConfiguration config;
    private AeroConnect connect;

    private final EdgeModel edgeModel;
    private final VertexModel vertexModel;

    private final AeroFeatures features;

    /*
     * The internal vertex and edge cache to accelerate
     * lookup (or read) operations without the need to
     * access Aerospike
     */
    private final Cache<ByteBuffer, Edge> edgeCache;
    private final Cache<ByteBuffer, Vertex> vertexCache;

    /**
     * This method is invoked by Gremlin's GraphFactory
     * and defines the starting point for further tasks.
     */
    public static AeroGraph open(final Configuration properties) throws GraphException {
        return new AeroGraph(properties);
    }

    public AeroGraph(Configuration properties) {
        this(new AeroConfiguration(properties));
    }

    public AeroGraph(AeroConfiguration config) throws GraphException {

        this.config = config;
        this.features = new AeroFeatures(true);

        /* Build Aerospike configuration */
        AeroConnect connect = AeroConnect.getInstance(config);

        /* SECONDARY INDEX CREATION */

        AeroIndex aeroIndex = new AeroIndex(connect);
        aeroIndex.edgeIndices();
        aeroIndex.vertexIndices();

        /* EDGE INITIALIZATION */

        String edgeSetname = connect.setname() + "_" + Constants.EDGES;
        this.edgeModel = new EdgeModel(this, new AeroTable(edgeSetname, connect));

        this.edgeCache = CacheBuilder.newBuilder()
                .maximumSize(this.config.getElementCacheMaxSize())
                .expireAfterAccess(this.config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<ByteBuffer, Edge>) listener -> ((AeroEdge) listener.getValue()).setCached(false))
                .build();

        /* VERTEX INITIALIZATION */

        String vertexSetname = connect.setname() + "_" + Constants.VERTICES;

        this.vertexModel = new VertexModel(this, new AeroTable(vertexSetname, connect));

        this.vertexCache = CacheBuilder.newBuilder()
                .maximumSize(this.config.getElementCacheMaxSize())
                .expireAfterAccess(this.config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<ByteBuffer, Vertex>) listener -> ((AeroVertex) listener.getValue()).setCached(false))
                .build();

    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public void close() throws Exception {
        /* Close AerospikeClient */
        this.connect.close();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public AeroConfiguration configuration() {
        return this.config;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, AeroConfiguration.AERO_GRAPH_CLASS.getSimpleName().toLowerCase());
    }

    /** VERTEX METHODS (see VertexModel) **/

    public VertexModel getVertexModel() {
        return vertexModel;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        /*
         * Vertices that define an `id` in the provided keyValues
         * use this value (long or numeric). Otherwise `null` is
         * returned.
         */
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        /*
         * The `idValue` either is a provided [Long] or a random
         * UUID as [String].
         */
        idValue = GraphUtils.generateIdIfNeeded(idValue);

        long now = System.currentTimeMillis();
        AeroVertex newVertex = new AeroVertex(this, idValue, label, now, now, GraphUtils.propertiesToMap(keyValues));

        newVertex.validate();
        newVertex.writeToModel();

        Vertex vertex = findOrCreateVertex(idValue);
        ((AeroVertex) vertex).copyFrom(newVertex);

        return vertex;

    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {

        if (vertexIds.length == 0) {
            return allVertices();

        } else {
            Stream<Object> stream = Stream.of(vertexIds);
            List<Vertex> vertices = stream
                    .map(id -> {
                        if (id == null)
                            throw Exceptions.argumentCanNotBeNull("id");
                        else if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Vertex)
                            return ((Vertex) id).id();
                        else
                            return id;
                    })
                    .map(this::findOrCreateVertex)
                    .collect(Collectors.toList());
            getVertexModel().load(vertices);
            return vertices.stream()
                    .filter(v -> ((AeroVertex) v).arePropertiesFullyLoaded())
                    .iterator();
        }
    }

    public Vertex vertex(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Vertex v = findOrCreateVertex(id);
        ((AeroVertex) v).load();
        return v;
    }

    public Vertex findOrCreateVertex(Object id) {
        return findVertex(id, true);
    }

    /**
     * Retrieve vertex from cache or build new
     * vertex instance with the provided `id`.
     */
    protected Vertex findVertex(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = GraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Vertex cachedVertex = vertexCache.getIfPresent(key);
        if (cachedVertex != null && !((AeroVertex) cachedVertex).isDeleted()) {
            return cachedVertex;
        }
        if (!createIfNotFound) return null;
        AeroVertex vertex = new AeroVertex(this, id);
        vertexCache.put(key, vertex);
        vertex.setCached(true);
        return vertex;
    }

    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }

    public Iterator<Vertex> allVertices() {
        return vertexModel.vertices();
    }

    public Iterator<Vertex> allVertices(Object fromId, int limit) {
        return vertexModel.vertices(fromId, limit);
    }

    public Iterator<Vertex> verticesByLabel(String label) {
        return vertexModel.vertices(label);
    }

    public Iterator<Vertex> verticesByLabel(String label, String key, Object value) {
        return vertexModel.vertices(label, key, value);
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return vertexModel.verticesInRange(label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit) {
        return vertexModel.verticesWithLimit(label, key, fromValue, limit);
    }

    /** EDGE METHODS (see EdgeModel) **/

    public EdgeModel getEdgeModel() {
        return edgeModel;
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {

        if (edgeIds.length == 0) {
            return allEdges();
        } else {
            Stream<Object> stream = Stream.of(edgeIds);
            List<Edge> edges = stream
                    .map(id -> {
                        if (id == null)
                            throw Exceptions.argumentCanNotBeNull("id");
                        else if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Edge)
                            return ((Edge) id).id();
                        else
                            return id;
                    })
                    .map(this::findOrCreateEdge)
                    .collect(Collectors.toList());
            getEdgeModel().load(edges);
            return edges.stream()
                    .filter(e -> ((AeroEdge) e).arePropertiesFullyLoaded())
                    .iterator();
        }

    }

    public Edge edge(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Edge edge = findOrCreateEdge(id);
        ((AeroEdge) edge).load();
        return edge;
    }

    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        return outVertex.addEdge(label, inVertex, keyValues);
    }

    public Edge findOrCreateEdge(Object id) {
        return findEdge(id, true);
    }

    protected Edge findEdge(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = GraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Edge cachedEdge = edgeCache.getIfPresent(key);
        if (cachedEdge != null && !((AeroEdge) cachedEdge).isDeleted()) {
            return cachedEdge;
        }
        if (!createIfNotFound) {
            return null;
        }
        AeroEdge edge = new AeroEdge(this, id);
        edgeCache.put(key, edge);
        edge.setCached(true);
        return edge;
    }

    public void removeEdge(Edge edge) {
        edge.remove();
    }

    public Iterator<Edge> allEdges() {
        return edgeModel.edges();
    }

    public Iterator<Edge> allEdges(Object fromId, int limit) {
        return edgeModel.edges(fromId, limit);
    }

}
