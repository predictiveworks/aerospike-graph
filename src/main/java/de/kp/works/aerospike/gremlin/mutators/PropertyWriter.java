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

import de.kp.works.aerospike.mutate.AeroMutation;
import de.kp.works.aerospike.mutate.AeroPut;
import de.kp.works.aerospike.gremlin.*;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyWriter implements Mutator {

    private final Element element;
    private final ElementType elementType;

    private final String key;
    private final Object value;

    public PropertyWriter(AeroGraph graph, Element element, ElementType elementType, String key, Object value) {
        this.element = element;
        this.elementType = elementType;

        this.key = key;
        this.value = value;
    }

    @Override
    public Iterator<AeroMutation> constructMutations() {

        Object id = element.id();
        AeroPut put = new AeroPut(id, elementType);

        put.addColumn(Constants.ID_COL_NAME, ValueUtils.getValueType(id).name(),
                id.toString());

        Long updatedAt = ((GraphElement) element).updatedAt();
        put.addColumn(Constants.UPDATED_AT_COL_NAME, Constants.LONG_COL_TYPE,
                updatedAt.toString());

        /* Put property */

        String colType = ValueUtils.getValueType(value).name();
        String colValue = value.toString();

        put.addColumn(key, colType, colValue);
        return IteratorUtils.of(put);

    }
}
