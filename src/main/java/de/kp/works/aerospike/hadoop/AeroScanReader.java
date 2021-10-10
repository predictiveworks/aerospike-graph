package de.kp.works.aerospike.hadoop;
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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ScanPolicy;
import de.kp.works.aerospike.util.NamedThreadFactory;

import java.util.Iterator;

public class AeroScanReader {

    private final String node;
    private final AeroConfig config;

    private final AeroScanIterator scanIterator = new AeroScanIterator();


    public AeroScanReader(String node, AeroConfig config) {
        this.node = node;
        this.config = config;
    }

    public Iterator<AeroKeyRecord> run(NamedThreadFactory scanThreadFactory) throws Exception {
        /*
         * The AeroScan implements a parallel scanning
         * approach
         */
        ScanPolicy scanPolicy = new ScanPolicy();

        scanPolicy.sendKey = true;
        scanPolicy.includeBinData = true;

        int timeout = config.getTimeout();
        scanPolicy.socketTimeout = timeout;
        scanPolicy.totalTimeout = timeout;

        AerospikeClient client = AeroClient.getInstance(config);

        String namespace = config.getNamespace();
        String setname = config.getSetname();

        Thread scanThread = scanThreadFactory.newThread(
            () -> {
                try {
                   client.scanNode(scanPolicy, node, namespace, setname, scanIterator);

                } finally {
                    scanIterator.close();
                }

            }
        );

        scanIterator.setThread(scanThread);
        scanThread.start();

        return scanIterator;

    }

}
