package de.kp.works.aerospikegraph.mutators;
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

import de.kp.works.aerospike.mutate.AeroPut;
import de.kp.works.aerospikegraph.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class EdgeWriter implements Creator {

    private final Edge edge;

    public EdgeWriter(AeroGraph graph, Edge edge) {
        this.edge = edge;
    }

    @Override
    public Edge getElement() {
        return edge;
    }

    @Override
    public Iterator<AeroPut> constructInsertions() {

        final String label = edge.label() != null ? edge.label() : Edge.DEFAULT_LABEL;

        Object id = edge.id();
        AeroPut put = new AeroPut(id, ElementType.EDGE);

        put.addColumn(Constants.ID_COL_NAME, ValueUtils.getValueType(id).name(),
                id.toString());

        put.addColumn(Constants.LABEL_COL_NAME, Constants.STRING_COL_TYPE,
                label);

        Object toId = edge.inVertex().id();
        put.addColumn(Constants.TO_COL_NAME, ValueUtils.getValueType(toId).name(),
                toId.toString());

        Object fromId = edge.outVertex().id();
        put.addColumn(Constants.FROM_COL_NAME, ValueUtils.getValueType(fromId).name(),
                fromId.toString());

        Long createdAt = ((AeroEdge) edge).createdAt();
        put.addColumn(Constants.CREATED_AT_COL_NAME, Constants.LONG_COL_TYPE,
                createdAt.toString());

        Long updatedAt = ((AeroEdge) edge).updatedAt();
        put.addColumn(Constants.UPDATED_AT_COL_NAME, Constants.LONG_COL_TYPE,
                updatedAt.toString());

        ((AeroEdge) edge).getProperties().forEach((key, value) -> {
            String colType = ValueUtils.getValueType(value).name();
            String colValue = value.toString();

            put.addColumn(key, colType, colValue);
        });

        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
    }
}
