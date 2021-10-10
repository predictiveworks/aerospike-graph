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

// TODO RELOAD & CLEAN PARAMETERS
public class AeroSplit extends InputSplit implements
        org.apache.hadoop.mapred.InputSplit {

    private String type;
    private String node;
    private String host;
    private int port;
    private String namespace;
    private String setname;
    private String[] binNames;
    private String numRangeBin;
    private long numRangeBegin;
    private long numRangeEnd;

    private AeroConfig config;

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
        this.type = config.getInputOperation();

        this.namespace = config.getInputNamespace();
        this.setname = config.getInputSetName();
        this.binNames = config.getInputBinNames();

        this.numRangeBin = config.getInputNumRangeBin();
        this.numRangeBegin = config.getInputNumRangeBegin();
        this.numRangeEnd = config.getInputNumRangeEnd();
    }

    public AeroConfig getConfig() {
        return config;
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

    public String getNameSpace() {
        return namespace;
    }

    public String getSetname() {
        return setname;
    }

    public String[] getBinNames() {
        return binNames;
    }

    public String getNumRangeBin() {
        return numRangeBin;
    }

    public long getNumRangeBegin() {
        return numRangeBegin;
    }

    public long getNumRangeEnd() {
        return numRangeEnd;
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
        if (binNames == null) {
            out.writeInt(0);
        } else {
            out.writeInt(binNames.length);
            for (String binName : binNames)
                Text.writeString(out, binName);
        }
        Text.writeString(out, numRangeBin);
        out.writeLong(numRangeBegin);
        out.writeLong(numRangeEnd);
    }

    public void readFields(DataInput in) throws IOException {
        type = Text.readString(in);
        node = Text.readString(in);
        host = Text.readString(in);
        port = in.readInt();
        namespace = Text.readString(in);
        setname = Text.readString(in);
        int nBinNames = in.readInt();
        if (nBinNames == 0) {
            binNames = null;
        } else {
            binNames = new String[nBinNames];
            for (int ii = 0; ii < nBinNames; ++ii)
                binNames[ii] = Text.readString(in);
        }
        numRangeBin = Text.readString(in);
        numRangeBegin = in.readLong();
        numRangeEnd = in.readLong();
    }

    public String[] getLocations() {
        return new String[] { host };
    }
}
