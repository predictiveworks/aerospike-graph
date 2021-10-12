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
import de.kp.works.aerospike.gremlin.AeroGraph;
import de.kp.works.aerospike.gremlin.AeroVertex;
import de.kp.works.aerospike.gremlin.Constants;
import de.kp.works.aerospike.gremlin.exception.GraphNotFoundException;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;
/**
 * [VertexReader] retrieves a query result and transforms
 * the data into a vertex representation.
 */
public class VertexReader extends LoadingElementReader<Vertex> {

    public VertexReader(AeroGraph graph) {
        super(graph);
    }

    @Override
    public Vertex parse(AeroResult result) {
        /*
         * Retrieve a vertex template that matches
         * the provided id, either from cache or
         * as a new one and load respective fields
         * from query result.
         */
        Object id = result.getId();
        Vertex vertex = graph.findOrCreateVertex(id);
        load(vertex, result);
        return vertex;
    }

    @Override
    public void load(Vertex vertex, AeroResult result) {
        if (result.isEmpty()) {
            throw new GraphNotFoundException(vertex, "Vertex does not exist: " + vertex.id());
        }
        String label   = null;
        Long createdAt = null;
        Long updatedAt = null;

        Map<String, Object> props = new HashMap<>();
        for (AeroColumn column : result.getColumns()) {
            String colName = column.getColName();
            switch (colName) {
                case Constants.LABEL_COL_NAME:
                    label = column.getColValue().toString();
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

        AeroVertex newVertex = new AeroVertex(graph, vertex.id(), label, createdAt, updatedAt, props);
        ((AeroVertex) vertex).copyFrom(newVertex);
    }
}
