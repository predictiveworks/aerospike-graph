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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class GraphUtils {
    /**
     * The Aerospike server configuration determines the supported
     * namespaces; sets and bins will be created on the file and
     * must not created like database tables.
     */
    public static Object generateIdIfNeeded(Object id) {

        if (id == null) {
            id = UUID.randomUUID().toString();

        } else if (id instanceof Long) {
            // noop

        } else if (id instanceof Number) {
            id = ((Number) id).longValue();

        }

        return id;
    }

    public static Map<String, Object> propertiesToMap(Object... keyValues) {
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object value = keyValues[i + 1];
            if (value == null) continue;
            ElementHelper.validateProperty(keyStr, value);
            props.put(keyStr, value);
        }
        return props;
    }

    public static Map<String, ValueType> propertyKeysAndTypesToMap(Object... keyTypes) {
        Map<String, ValueType> props = new HashMap<>();
        for (int i = 0; i < keyTypes.length; i = i + 2) {
            Object key = keyTypes[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object type = keyTypes[i + 1];
            ValueType valueType;
            if (type instanceof ValueType) {
                valueType = (ValueType) type;
            } else {
                valueType = ValueType.valueOf(type.toString().toUpperCase());
            }
            props.put(keyStr, valueType);
        }
        return props;
    }

}
