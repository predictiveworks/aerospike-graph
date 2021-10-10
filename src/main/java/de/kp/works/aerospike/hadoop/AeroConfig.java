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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class AeroConfig {

    private static final Log log = LogFactory.getLog(AeroConfig.class);

    private final String host;
    private final int port;
    private final String type;
    private final String namespace;
    private final String setname;
    private final int timeout;

    private final String username;
    private final String password;

    private final String authMode;

    private final String tlsMode;
    private final String tlsName;

    /**
     * The Aerospike authentication mode. Values are
     * INTERNAL, EXTERNAL, EXTERNAL_INSECURE, PKI.
     *
     * Default is INTERNAL
     */
    public static final String AEROSPIKE_AUTH_MODE = "aerospike.auth.mode";
    public static final String DEFAULT_AEROSPIKE_AUTH_MODE = "INTERNAL";

    public static final String AEROSPIKE_HOST = "aerospike.host";
    public static final String DEFAULT_AEROSPIKE_HOST = "localhost";

    public static final String AEROSPIKE_PORT = "aerospike.port";
    public static final int DEFAULT_AEROSPIKE_PORT = 3000;
    /**
     * The name of the Aerospike namespace used
     * to organize data
     */
    public static final String AEROSPIKE_NAMESPACE = "aerospike.namespace";
    public static final String AEROSPIKE_SETNAME = "aerospike.setname";

     public static final String AEROSPIKE_OPERATION = "aerospike.operation";
    public static final String DEFAULT_AEROSPIKE_OPERATION = "scan";
    /**
     * The timeout of an Aerospike database connection
     * in milliseconds. Default is 1000.
     */
    public static final String AEROSPIKE_TIMEOUT = "aerospike.timeout";
    public static final int DEFAULT_AEROSPIKE_TIMEOUT = 1000;
    /**
     * Password of the registered user. Required for authentication
     */
    public static final String AEROSPIKE_PASSWORD = "aerospike.password";
    /**
     * Name of a registered user name. Required for authentication
     */
    public static final String  AEROSPIKE_USER = "aerospike.username";

    public static final String AEROSPIKE_TLS_MODE = "aerospike.tls.mode";
    public static final String DEFAULT_AEROSPIKE_TLS_MODE = "false";

    public static final String AEROSPIKE_TLS_NAME = "aerospike.tls.name";

    public AeroConfig(Configuration conf) {

        this.host = conf.get(AEROSPIKE_HOST, DEFAULT_AEROSPIKE_HOST);
        log.info("using " + AEROSPIKE_HOST + " = " + host);

        this.port = conf.getInt(AEROSPIKE_PORT, DEFAULT_AEROSPIKE_PORT);
        log.info("using " + AEROSPIKE_PORT + " = " + port);

        this.type = conf.get(AEROSPIKE_OPERATION, DEFAULT_AEROSPIKE_OPERATION);
        log.info("using " + AEROSPIKE_OPERATION + " = " + type);

        this.namespace = conf.get(AEROSPIKE_NAMESPACE);
        if (namespace == null)
            throw new UnsupportedOperationException(
                    "No Aerospike namespace specified.");
        log.info("using " + AEROSPIKE_NAMESPACE + " = " + namespace);

        this.setname = conf.get(AEROSPIKE_SETNAME);
        log.info("using " + AEROSPIKE_SETNAME + " = " + setname);

        this.timeout = conf.getInt(AEROSPIKE_TIMEOUT, DEFAULT_AEROSPIKE_TIMEOUT);
        log.info("using " + AEROSPIKE_TIMEOUT + " = " + timeout);

        this.username = conf.get(AEROSPIKE_USER, null);
        log.info("using " + AEROSPIKE_USER + " = " + username);

        this.password = conf.get(AEROSPIKE_PASSWORD, null);
        log.info("using " + AEROSPIKE_PASSWORD + " = " + password);

        this.authMode = conf.get(AEROSPIKE_AUTH_MODE, DEFAULT_AEROSPIKE_AUTH_MODE);
        log.info("using " + AEROSPIKE_AUTH_MODE + " = " + authMode);

        this.tlsMode = conf.get(AEROSPIKE_TLS_MODE, DEFAULT_AEROSPIKE_TLS_MODE);
        log.info("using " + AEROSPIKE_TLS_MODE + " = " + tlsMode);

        this.tlsName = conf.get(AEROSPIKE_TLS_NAME, null);
        log.info("using " + AEROSPIKE_TLS_NAME + " = " + tlsName);

    }

    public AeroConfig(
            String host,
            int port,
            int timeout,
            String type,
            String namespace,
            String setname,
            String username,
            String password,
            String authMode,
            String tlsMode,
            String tlsName) {

        this.host = host;
        this.port = port;

        this.type = type;

        this.namespace = namespace;
        this.setname = setname;
        this.timeout = timeout;

        this.username = username;
        this.password = password;

        this.authMode = authMode;
        this.tlsMode = tlsMode;
        this.tlsName = tlsName;

    }

    public String getAuthMode() {
        return authMode;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSetname() {
        return setname;
    }

    public String getOperation() {
        return type;
    }

    public String getTlsMode() {
        return tlsMode;
    }

    public String getTlsName() {
        return tlsName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getTimeout() {
        return timeout;
    }

}