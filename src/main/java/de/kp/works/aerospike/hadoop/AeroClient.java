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
import com.aerospike.client.Host;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

import java.io.IOException;

public class AeroClient {

    private static volatile AerospikeClient instance = null;

    public static AerospikeClient getInstance(AeroConfig conf) throws Exception {
        if (instance == null) {
            synchronized (AeroClient.class) {
                if (instance == null) {

                    /* Define Client Policy */

                    ClientPolicy clientPolicy = new ClientPolicy();
                    clientPolicy.timeout = conf.getTimeout();
                    clientPolicy.failIfNotConnected = true;

                    /* User authentication */

                    String username = conf.getUsername();
                    String password = conf.getPassword();

                    if (username != null && password != null) {
                        clientPolicy.user = username;
                        clientPolicy.password = password;
                    }

                    String authValue = conf.getAuthMode();
                    clientPolicy.authMode = AuthMode.valueOf(authValue.toUpperCase());

                    String tlsMode = conf.getTlsMode().toLowerCase();
                    String tlsName = conf.getTlsName();

                    if (tlsMode.equals("true") && tlsName == null)
                        throw new Exception("No Aerospike TLS name specified.");

                    if (tlsMode.equals("true")) {
                        /*
                         * The current implementation leverages the
                         * default values
                         */
                        clientPolicy.tlsPolicy = new TlsPolicy();
                    }

                    String host = conf.getHost();
                    int port = conf.getPort();

                    Host aerospikeHost = new Host(host, tlsName, port);
                    instance = new AerospikeClient(clientPolicy, aerospikeHost);

                }
            }
        }

        return instance;
    }
}
