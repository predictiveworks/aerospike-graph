package de.kp.works.aerospike.gremlin;
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

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;
import java.util.Iterator;

public class AeroConfiguration extends AbstractConfiguration implements Serializable {

    private static final long serialVersionUID = -7150699702127992270L;

    private final PropertiesConfiguration conf;

    public static final Class<? extends Graph> AERO_GRAPH_CLASS = AeroGraph.class;

    public static final String AERO_GRAPH_CLASSNAME = AERO_GRAPH_CLASS.getCanonicalName();

    public static class Keys {
        /**
         * The Aerospike authentication mode. Values are
         * INTERNAL, EXTERNAL, EXTERNAL_INSECURE, PKI.
         *
         * Default is INTERNAL
         */
        public static final String AEROSPIKE_AUTH_MODE = "aerospike.auth.mode";
        /**
         * Record expiration. Also known as ttl (time to live).
         * Seconds record will live before being removed by the server.
         *
         * Expiration values:
         *
         *  -2: Do not change ttl when record is updated.
         *  -1: Never expire.
         *   0: Default to namespace configuration variable "default-ttl" on the server.
         * > 0: Actual ttl in seconds.
         *
         * Default: 0
         */
        public static final String AEROSPIKE_EXPIRATION = "aerospike.expiration";
        /**
         * The host of the Aerospike database
         */
        public static final String AEROSPIKE_HOST = "aerospike.host";
        /**
         * The name of the Aerospike namespace used
         * to organize data
         */
        public static final String AEROSPIKE_NAMESPACE = "aerospike.namespace";
        /**
         * Password of the registered user.
         * Required for authentication
         */
        public static final String AEROSPIKE_PASSWORD = "aerospike.password";
        /**
         * The port of the Aerospike database
         */
        public static final String AEROSPIKE_PORT = "aerospike.port";
        /**
         * The name of the Aerospike set used to
         * organize data
         */
        public static final String AEROSPIKE_SET      = "aerospike.set";
        public static final String AEROSPIKE_TIMEOUT  = "aerospike.timeout";
        public static final String AEROSPIKE_TLS_MODE = "aerospike.tls.mode";
        public static final String AEROSPIKE_TLS_NAME = "aerospike.tls.name";
        /**
         * Name of a registered user name.
         * Required for authentication
         */
        public static final String  AEROSPIKE_USER  = "aerospike.username";
        public static final String  AEROSPIKE_WRITE = "aerospike.write";

        public static final String GRAPH_CLASS = "gremlin.graph";
        /**
         * Edge & vertex cache configuration
         */
        public static final String GLOBAL_CACHE_MAX_SIZE = "global.cache.max.size";
        public static final String GLOBAL_CACHE_TTL_SECS = "global.cache.ttl.secs";

        public static final String EDGE_CACHE_MAX_SIZE = "edge.cache.max.size";
        public static final String EDGE_CACHE_TTL_SECS = "edge.cache.ttl.secs";

    }

    /**
     * A minimal configuration for the AeroGraph
     */
    public AeroConfiguration() {
        conf = new PropertiesConfiguration();
        conf.setProperty(AeroConfiguration.Keys.GRAPH_CLASS, AERO_GRAPH_CLASSNAME);
    }

    public AeroConfiguration(Configuration config) {

        conf = new PropertiesConfiguration();
        conf.setProperty(AeroConfiguration.Keys.GRAPH_CLASS, AERO_GRAPH_CLASSNAME);
        if (config != null) {
            config.getKeys().forEachRemaining(key ->
                    conf.setProperty(key.replace("..", "."), config.getProperty(key)));
        }
    }

    public long getEdgeCacheMaxSize() {
        return conf.getLong(AeroConfiguration.Keys.EDGE_CACHE_MAX_SIZE, 1000);
    }

    public long getEdgeCacheTtlSecs() {
        return conf.getLong(AeroConfiguration.Keys.EDGE_CACHE_TTL_SECS, 60);
    }

    public long getElementCacheMaxSize() {
        return conf.getLong(AeroConfiguration.Keys.GLOBAL_CACHE_MAX_SIZE, 1000000);
    }

    public long getElementCacheTtlSecs() {
        return conf.getLong(AeroConfiguration.Keys.GLOBAL_CACHE_TTL_SECS, 60);
    }

    public PropertiesConfiguration getConf() {
        return conf;
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        conf.setProperty(key, value);
    }

    @Override
    protected void clearPropertyDirect(String key) {
        conf.clearProperty(key);
    }

    @Override
    protected Iterator<String> getKeysInternal() {
        return conf.getKeys();
    }

    @Override
    protected Object getPropertyInternal(String key) {
        return conf.getProperty(key);
    }

    public AeroConfiguration set(String key, Object value) {
        conf.setProperty(key, value);
        return this;
    }

    @Override
    protected boolean isEmptyInternal() {
        return conf.isEmpty();
    }

    @Override
    protected boolean containsKeyInternal(String key) {
        return conf.containsKey(key);
    }

}
