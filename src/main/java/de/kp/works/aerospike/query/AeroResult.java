package de.kp.works.aerospike.query;
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
import de.kp.works.aerospikegraph.Constants;

import java.util.ArrayList;
import java.util.List;

public class AeroResult {
    /**
     * [IgniteResult] represents all cache entries
     * that refer to a certain edge, row, etc.
     */
    public AeroResult() {}

    private List<AeroColumn> columns = new ArrayList<>();

    public void addColumn(String colName, String colType, String colValue) {
        columns.add(new AeroColumn(colName, colType, colValue));
    }

    /**
     * This method returns the user identifier assigned
     * to edges, vertices and other
     */
    public Object getId() {
        return getValue(Constants.ID_COL_NAME);
    }

    public Object getValue(String key) {

        Object value = null;
        for (AeroColumn column : columns) {
            if (column.getColName().equals(key)) {
                value = column.getColValue();
            }
        }

        return value;

    }

    public List<AeroColumn> getColumns() {
        return columns;
    }

    public boolean isEmpty() {
         return columns.isEmpty();
    }
}
