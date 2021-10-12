package de.kp.works.aerospike.mutate;
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
import de.kp.works.aerospike.gremlin.Constants;
import de.kp.works.aerospike.gremlin.ElementType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AeroMutation {

    public Object id;
    public List<AeroColumn> columns = new ArrayList<>();

    public ElementType elementType;
    public AeroMutationType mutationType;

    public AeroMutation(Object id) {
        this.id = id;
    }

    public List<AeroColumn> getColumns() {
        return columns;
    }

    public List<String> getColumnNames() {
        return columns
                .stream().map(AeroColumn::getColName)
                .collect(Collectors.toList());
    }

    public AeroColumn getColumn(String columnName) {

        List<AeroColumn> filtered = columns
                .stream()
                .filter((column) ->
                        column.getColName().equals(columnName)).collect(Collectors.toList());

        if (filtered.isEmpty()) return null;
        return filtered.get(0);

    }

    public Stream<AeroColumn> getProperties() {
        return columns.stream()
                .filter(column -> {
                    switch (column.getColName()) {
                        case Constants.ID_COL_NAME:
                        case Constants.LABEL_COL_NAME:
                        case Constants.TO_COL_NAME:
                        case Constants.FROM_COL_NAME:
                        case Constants.CREATED_AT_COL_NAME:
                        case Constants.UPDATED_AT_COL_NAME:
                            return false;

                        default:
                            return true;
                    }
                });
    }

    public Object getId() {
        return id;
    }

}
