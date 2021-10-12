package de.kp.works.aerospike.gremlin.models;
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

import de.kp.works.aerospike.AeroTable;
import de.kp.works.aerospike.gremlin.AeroGraph;

public abstract class BaseModel {

    protected final AeroGraph graph;
    protected final AeroTable table;

    public BaseModel(AeroGraph graph, AeroTable table) {
        this.graph = graph;
        this.table = table;
    }

    public AeroGraph getGraph() {
        return graph;
    }

    public AeroTable getTable() {
        return table;
    }

}
