package de.kp.works.aerospike.gremlin.mutators;
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
import de.kp.works.aerospike.gremlin.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public final class VertexWriter implements Creator {

    private final Vertex vertex;

    public VertexWriter(AeroGraph graph, Vertex vertex) {
        this.vertex = vertex;
    }

    @Override
    public Vertex getElement() {
        return vertex;
    }

    @Override
    public Iterator<AeroPut> constructInsertions() {

        final String label = vertex.label() != null ? vertex.label() : Vertex.DEFAULT_LABEL;

        Object id = vertex.id();

        AeroPut put = new AeroPut(id, ElementType.VERTEX);
        put.addColumn(Constants.ID_COL_NAME, ValueUtils.getValueType(id).name(),
                id.toString());

        put.addColumn(Constants.LABEL_COL_NAME, Constants.STRING_COL_TYPE,
                label);

        Long createdAt = ((AeroVertex) vertex).createdAt();
        put.addColumn(Constants.CREATED_AT_COL_NAME, Constants.LONG_COL_TYPE,
                createdAt.toString());

        Long updatedAt = ((AeroVertex) vertex).updatedAt();
        put.addColumn(Constants.UPDATED_AT_COL_NAME, Constants.LONG_COL_TYPE,
                updatedAt.toString());

        ((AeroVertex) vertex).getProperties().forEach((key, value) -> {
            String colType = ValueUtils.getValueType(value).name();
            String colValue = value.toString();

            put.addColumn(key, colType, colValue);
        });

        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
    }
}
