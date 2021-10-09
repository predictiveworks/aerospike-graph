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

import de.kp.works.aerospike.AeroTable;
import de.kp.works.aerospike.mutate.AeroMutation;
import de.kp.works.aerospike.mutate.AeroPut;
import de.kp.works.aerospike.query.AeroResult;
import de.kp.works.aerospikegraph.exception.GraphException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Mutators {

    public static void create(AeroTable table, Creator... creators) {

        List<AeroMutation> batch = new ArrayList<>();
        for (Creator creator : creators) {
            Iterator<AeroPut> insertions = creator.constructInsertions();
            insertions.forEachRemaining(batch::add);
        }
        write(table, batch);
    }

    private static void create(AeroTable table, Creator creator, AeroPut put) {

        boolean success = false;
        try {
            success = table.put(put);
        } catch (Exception e) {
            /* Do nothing */
        }

        if (!success) {
            throw creator.alreadyExists();
        }
    }

    public static void write(AeroTable table, Mutator... writers) {
        List<AeroMutation> batch = new ArrayList<>();
        for (Mutator writer : writers) {
            writer.constructMutations().forEachRemaining(batch::add);
        }
        write(table, batch);
    }

    public static long increment(AeroTable table, Mutator writer, String key) {

        List<AeroMutation> batch = new ArrayList<>();
        writer.constructMutations().forEachRemaining(batch::add);

        Object[] results = write(table, batch);

        // Increment result is the first
        AeroResult result = (AeroResult) results[0];
        Object value = result.getValue(key);

        if (value instanceof Exception) {
            throw new GraphException((Exception) value);
        }

        return (long)value;
    }

    private static Object[] write(AeroTable table, List<AeroMutation> mutations) {

        Object[] results = new Object[mutations.size()];
        if (mutations.isEmpty()) return results;

        try {
            table.batch(mutations, results);

            for (Object result : results) {
                if (result instanceof Exception) {
                    throw new GraphException((Exception) result);
                }
            }

        } catch (Exception e) {
            throw new GraphException(e);
        }

        return results;

    }
}
