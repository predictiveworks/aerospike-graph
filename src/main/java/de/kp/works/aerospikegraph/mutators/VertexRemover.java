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

import de.kp.works.aerospike.mutate.AeroDelete;
import de.kp.works.aerospike.mutate.AeroMutation;
import de.kp.works.aerospikegraph.AeroGraph;
import de.kp.works.aerospikegraph.ElementType;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public final class VertexRemover implements Mutator {

    private final Vertex vertex;

    public VertexRemover(AeroGraph graph, Vertex vertex) {
        this.vertex = vertex;
    }

    @Override
    public Iterator<AeroMutation> constructMutations() {
       AeroDelete delete = new AeroDelete(vertex.id(), ElementType.VERTEX);
       return IteratorUtils.of(delete);
    }
}
