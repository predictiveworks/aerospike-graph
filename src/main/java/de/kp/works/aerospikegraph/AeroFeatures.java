package de.kp.works.aerospikegraph;
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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

public class AeroFeatures implements Graph.Features {

    protected final boolean supportsPersistence;
    protected final GraphFeatures graphFeatures = new AeroGraphFeatures();
    protected final VertexFeatures vertexFeatures = new AeroVertexFeatures();
    protected final EdgeFeatures edgeFeatures = new AeroEdgeFeatures();

    public AeroFeatures(boolean supportsPersistence) {
        this.supportsPersistence = supportsPersistence;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class AeroGraphFeatures implements GraphFeatures {

        private final VariableFeatures variableFeatures = new AeroVariableFeatures();

        AeroGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return true;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return supportsPersistence;
        }

        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public static class AeroVertexFeatures extends AeroElementFeatures implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures = new AeroVertexPropertyFeatures();

        AeroVertexFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMetaProperties() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    public static class AeroEdgeFeatures extends AeroElementFeatures implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures = new AeroEdgePropertyFeatures();

        AeroEdgeFeatures() {
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }

    }

    public static class AeroElementFeatures implements ElementFeatures {

        AeroElementFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return id instanceof Serializable;
        }
    }

    public static class AeroVertexPropertyFeatures implements VertexPropertyFeatures {

        AeroVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsSerializableValues() {
            return true;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class AeroEdgePropertyFeatures implements EdgePropertyFeatures {

        AeroEdgePropertyFeatures() {
        }

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsSerializableValues() {
            return true;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

    }

    public static class AeroVariableFeatures implements Graph.Features.VariableFeatures {

        @Override
        public boolean supportsVariables() {
            return false;
        }

        @Override
        public boolean supportsBooleanValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleValues() {
            return false;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return false;
        }

        @Override
        public boolean supportsLongValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsByteValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }
}
