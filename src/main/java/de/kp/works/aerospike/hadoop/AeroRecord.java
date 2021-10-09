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

import com.aerospike.client.Record;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Unpacker;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AeroRecord implements Writable {

    public Map<String,Object> bins;
    public int generation;
    public int expiration;

    public AeroRecord() {
        this.bins = null;
        this.generation = 0;
        this.expiration = 0;
    }

    public AeroRecord(Record rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public AeroRecord(AeroRecord rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public void set(Record rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public void set(AeroRecord rec) {
        this.bins = rec.bins;
        this.generation = rec.generation;
        this.expiration = rec.expiration;
    }

    public Record toRecord() {
        return new Record(bins, generation, expiration);
    }

    public void write(DataOutput out) throws IOException {
        try {
            out.writeInt(generation);
            out.writeInt(expiration);
            out.writeInt(bins.size());
            for (Map.Entry<String, Object> entry : bins.entrySet()) {
                out.writeUTF(entry.getKey());
                Packer pack = new Packer();
                pack.packObject(entry.getValue());
                byte[] buff = pack.toByteArray();
                out.writeInt(buff.length);
                out.write(buff);
            }
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void readFields(DataInput in) throws IOException {
        try {
            generation = in.readInt();
            expiration = in.readInt();
            int nbins = in.readInt();
            bins = new HashMap<String, Object>();
            for (int ii = 0; ii < nbins; ++ii) {
                String key = in.readUTF();
                int bufflen = in.readInt();
                byte[] buff = new byte[bufflen];
                in.readFully(buff);
                Unpacker.ObjectUnpacker unpack = new Unpacker.ObjectUnpacker(buff, 0, buff.length);
                Object obj = unpack.unpackObject();
                bins.put(key, obj);
            }
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static AeroRecord read(DataInput in) throws IOException {
        AeroRecord rec = new AeroRecord();
        rec.readFields(in);
        return rec;
    }
}