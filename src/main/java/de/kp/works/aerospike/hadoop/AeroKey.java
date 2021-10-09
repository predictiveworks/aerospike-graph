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

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Unpacker;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("rawtypes")
public class AeroKey implements WritableComparable {

    public String namespace;
    public String setName;
    public byte[] digest;
    public Value userKey;

    public AeroKey() {
        this.namespace = null;
        this.setName = null;
        this.digest = null;
        this.userKey = null;
    }

    public AeroKey(Key key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public AeroKey(AeroKey key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public void set(Key key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public void set(AeroKey key) {
        this.namespace = key.namespace;
        this.digest = key.digest;
        this.setName = key.setName;
        this.userKey = key.userKey;
    }

    public Key toKey() {
        return new Key(namespace, digest, setName, userKey);
    }

    public void write(DataOutput out) throws IOException {
        try {
            out.writeUTF(namespace);
            out.writeUTF(setName);
            out.writeInt(digest.length);
            out.write(digest);
            out.writeBoolean(userKey != null);
            if (userKey == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Packer pack = new Packer();
                pack.packObject(userKey);
                byte[] buff = pack.toByteArray();
                out.writeInt(buff.length);
                out.write(buff);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void readFields(DataInput in) throws IOException {
        try {
            namespace = in.readUTF();
            setName = in.readUTF();
            int digestLen = in.readInt();
            digest = new byte[digestLen];
            in.readFully(digest);
            if (in.readBoolean()) {
                int bufflen = in.readInt();
                byte[] buff = new byte[bufflen];
                in.readFully(buff);
                Unpacker.ObjectUnpacker unpack = new Unpacker.ObjectUnpacker(buff, 0, buff.length);
                userKey = Value.get(unpack.unpackObject());
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static AeroKey read(DataInput in) throws IOException {
        AeroKey key = new AeroKey();
        key.readFields(in);
        return key;
    }

    public int compareTo(Object obj) {
        AeroKey other = (AeroKey) obj;
        byte[] left = this.digest;
        byte[] right = other.digest;
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

}
