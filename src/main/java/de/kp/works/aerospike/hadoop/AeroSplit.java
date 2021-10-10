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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AeroSplit extends InputSplit implements
        org.apache.hadoop.mapred.InputSplit {

    private String type;
    private String node;
    private String host;
    private int port;
    private String namespace;
    private String setname;
    private int timeout;

    private String username;
    private String password;

    private String authMode;
    private String tlsMode;
    private String tlsName;

    public AeroSplit(String node, String host, int port, AeroConfig config) {
        /*
         * Node specific parameters
         */
        this.node = node;
        this.host = host;
        this.port = port;
        /*
         * Common parameters
         */
        this.type = config.getOperation();

        this.namespace = config.getNamespace();
        this.setname = config.getSetname();

        this.timeout = config.getTimeout();

        this.username = config.getUsername();
        this.password = config.getPassword();

        this.authMode = config.getAuthMode();
        this.tlsMode = config.getTlsMode();
        this.tlsName = config.getTlsName();
    }

    public AeroConfig getConfig() {
        return new AeroConfig(
                host,
                port,
                timeout,
                type,
                namespace,
                setname,
                username,
                password,
                authMode,
                tlsMode,
                tlsName);
    }

    public String getType() {
        return type;
    }

    public String getNode() {
        return node;
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

    public long getLength() {
        return 1;
    }

    public String toString() {
        return type + ':' + node + ":" + host + ":" + port + ":" + namespace
                + ":" + setname;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type);
        Text.writeString(out, node);
        Text.writeString(out, host);
        out.writeInt(port);
        Text.writeString(out, namespace);
        Text.writeString(out, setname);
        out.writeInt(timeout);
        Text.writeString(out, username);
        Text.writeString(out, password);
        Text.writeString(out, authMode);
        Text.writeString(out, tlsMode);
        Text.writeString(out, tlsName);
    }

    public void readFields(DataInput in) throws IOException {

        type = Text.readString(in);
        node = Text.readString(in);
        host = Text.readString(in);
        port = in.readInt();
        namespace = Text.readString(in);
        setname = Text.readString(in);
        timeout = in.readInt();
        username = Text.readString(in);
        password = Text.readString(in);
        authMode = Text.readString(in);
        tlsMode = Text.readString(in);
        tlsName = Text.readString(in);

    }

    public String[] getLocations() {
        return new String[] { host };
    }
}
