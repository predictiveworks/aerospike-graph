package de.kp.works.aerospike.gremlin.readers;
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

import de.kp.works.aerospike.AeroColumn;
import de.kp.works.aerospike.query.AeroResult;
import de.kp.works.aerospike.gremlin.AeroEdge;
import de.kp.works.aerospike.gremlin.AeroGraph;
import de.kp.works.aerospike.gremlin.Constants;
import de.kp.works.aerospike.gremlin.exception.GraphNotFoundException;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.HashMap;
import java.util.Map;
/**
 * [EdgeReader] retrieves a query result and transforms
 * the data into an edge representation.
 */
public class EdgeReader extends LoadingElementReader<Edge> {

    public EdgeReader(AeroGraph graph) {
        super(graph);
    }

    @Override
    public Edge parse(AeroResult result) {
        /*
         * Retrieve an edge template that matches
         * the provided id, either from cache or
         * as a new one and load respective fields
         * from query result.
         */
        Object id = result.getId();
        Edge edge = graph.findOrCreateEdge(id);
        load(edge, result);

        return edge;
    }

    @Override
    public void load(Edge edge, AeroResult result) {
        if (result.isEmpty()) {
            throw new GraphNotFoundException(edge, "Edge does not exist: " + edge.id());
        }
        Object inVertexId = null;
        Object outVertexId = null;

        String label = null;

        Long createdAt = null;
        Long updatedAt = null;

        Map<String, Object> props = new HashMap<>();
        for (AeroColumn column : result.getColumns()) {
            String colName = column.getColName();
            switch (colName) {
                case Constants.LABEL_COL_NAME:
                    label = column.getColValue().toString();
                    break;
                case Constants.FROM_COL_NAME:
                    outVertexId = column.getColValue();
                    break;
                case Constants.TO_COL_NAME:
                    inVertexId = column.getColValue();
                    break;
                case Constants.CREATED_AT_COL_NAME:
                    createdAt = (Long)column.getColValue();
                    break;
                case Constants.UPDATED_AT_COL_NAME:
                    updatedAt = (Long)column.getColValue();
                    break;
                default:
                    props.put(colName, column.getColValue());
                    break;
            }

        }

        if (inVertexId != null && outVertexId != null && label != null) {
            AeroEdge newEdge = new AeroEdge(graph, edge.id(), label, createdAt, updatedAt, props,
                    graph.findOrCreateVertex(inVertexId),
                    graph.findOrCreateVertex(outVertexId));
            ((AeroEdge) edge).copyFrom(newEdge);
        } else {
            throw new IllegalStateException("Unable to parse edge from cells");
        }
    }
}
