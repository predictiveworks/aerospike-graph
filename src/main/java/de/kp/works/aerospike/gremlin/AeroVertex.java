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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import de.kp.works.aerospike.gremlin.exception.GraphNotFoundException;
import de.kp.works.aerospike.gremlin.models.VertexModel;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Tuple;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AeroVertex extends GraphElement implements Vertex {

    private transient Cache<Tuple, List<Edge>> edgeCache;

    public AeroVertex(AeroGraph graph, Object id) {
        this(graph, id, null, null, null, null, false);
    }

    public AeroVertex(AeroGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    public AeroVertex(AeroGraph graph, Object id, String label, Long createdAt, Long updatedAt,
                      Map<String, Object> properties, boolean propertiesFullyLoaded) {
        super(graph, id, label, createdAt, updatedAt, properties, propertiesFullyLoaded);

        if (graph != null) {
            this.edgeCache = CacheBuilder.newBuilder()
                    .maximumSize(graph.configuration().getEdgeCacheMaxSize())
                    .expireAfterAccess(graph.configuration().getEdgeCacheTtlSecs(), TimeUnit.SECONDS)
                    .build();
        }
    }

    @Override
    public void validate() {
        /* Do nothing */
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

    public Iterator<Edge> getEdgesFromCache(Tuple cacheKey) {
        if (edgeCache == null || !isCached()) return null;
        List<Edge> edges = edgeCache.getIfPresent(cacheKey);
        return edges != null ? IteratorUtils.filter(edges.iterator(), edge -> !((AeroEdge) edge).isDeleted()) : null;
    }

    public void cacheEdges(Tuple cacheKey, List<Edge> edges) {
        if (edgeCache == null || !isCached()) return;
        edgeCache.put(cacheKey, edges);
    }

    protected void invalidateEdgeCache() {
        if (edgeCache != null) edgeCache.invalidateAll();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        idValue = GraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        AeroEdge newEdge = new AeroEdge(graph, idValue, label, now, now, GraphUtils.propertiesToMap(keyValues), inVertex, this);
        newEdge.validate();
        newEdge.writeToModel();

        invalidateEdgeCache();
        if (!isCached()) {
            AeroVertex cachedVertex = (AeroVertex) graph.findVertex(id, false);
            if (cachedVertex != null) cachedVertex.invalidateEdgeCache();
        }
        ((AeroVertex) inVertex).invalidateEdgeCache();
        if (!((AeroVertex) inVertex).isCached()) {
            AeroVertex cachedInVertex = (AeroVertex) graph.findVertex(inVertex.id(), false);
            if (cachedInVertex != null) cachedInVertex.invalidateEdgeCache();
        }

        Edge edge = graph.findOrCreateEdge(idValue);
        ((AeroEdge) edge).copyFrom(newEdge);
        return edge;
    }

    @Override
    public void remove() {
        // Remove edges incident to this vertex.
        edges(Direction.BOTH).forEachRemaining(edge -> {
            try {
                edge.remove();
            } catch (GraphNotFoundException e) {
                // ignore
            }
        });

        // Get rid of the vertex.
        deleteFromModel();

        setDeleted(true);
        if (!isCached()) {
            AeroVertex cachedVertex = (AeroVertex) graph.findVertex(id, false);
            if (cachedVertex != null) cachedVertex.setDeleted(true);
        }
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        if (value != null) {
            setProperty(key, value);
            return new AeroVertexProperty<>(graph, this, key, value);
        } else {
            removeProperty(key);
            return VertexProperty.empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        V value = getProperty(key);
        return value != null ? new AeroVertexProperty<>(graph, this, key, value) : VertexProperty.empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        Iterable<String> keys = getPropertyKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new AeroVertexProperty<>(graph, this, key, getProperty(key)));
    }

    /** EDGE RELATED **/

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return graph.getEdgeModel().edges(this, direction, edgeLabels);
    }

    public Iterator<Edge> edges(final Direction direction, final String label, final String key, final Object value) {
        return graph.getEdgeModel().edges(this, direction, label, key, value);
    }

    public Iterator<Edge> edgesInRange(final Direction direction, final String label, final String key,
                                       final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeModel().edgesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    /** VERTEX RELATED **/

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return graph.getEdgeModel().vertices(this, direction, edgeLabels);
    }

    public Iterator<Vertex> vertices(final Direction direction, final String label, final String key, final Object value) {
        return graph.getEdgeModel().vertices(this, direction, label, key, value);
    }

    public Iterator<Vertex> verticesInRange(final Direction direction, final String label, final String key,
                                            final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeModel().verticesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    @Override
    public VertexModel getModel() {
        return graph.getVertexModel();
    }

    @Override
    public void writeToModel() {
        getModel().writeVertex(this);
    }

    @Override
    public void deleteFromModel() {
        getModel().deleteVertex(this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
