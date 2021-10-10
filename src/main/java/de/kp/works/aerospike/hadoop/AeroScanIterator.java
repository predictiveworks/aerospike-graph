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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class AeroScanIterator implements Iterator<AeroKeyRecord>, ScanCallback {

    private Thread thread;
    /*
     * A blocking queue to collect (key, record) pairs
     * from Aerospike while scanning.
     */
    private final BlockingQueue<AeroKeyRecord> queue = new LinkedBlockingQueue<AeroKeyRecord>(100);
    private AeroKeyRecord nextKeyRecord;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AeroKeyRecord TERMINATE_VALUE = new AeroKeyRecord(null, null);

    public AeroScanIterator() {
    }

    public void close() {

        closed.set(true);
        try {

            queue.put(TERMINATE_VALUE);
            thread.interrupt();

        } catch (InterruptedException e) {
            throw new RuntimeException();
        }

    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    @Override
    public void scanCallback(Key key, Record record) throws AerospikeException {
        if (closed.get()) {
            /*
             * Scan iterator get closed, so terminate scan
             */
            throw new AerospikeException.ScanTerminated();
        }
        try {
            queue.put(new AeroKeyRecord(new AeroKey(key), new AeroRecord(record)));

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean hasNext() {

        try {
            if (nextKeyRecord == TERMINATE_VALUE){
                return false;
            }

            return nextKeyRecord != null || (nextKeyRecord = takeNext()) != TERMINATE_VALUE;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public AeroKeyRecord next() {

        if (nextKeyRecord == null){

            try {
                nextKeyRecord = takeNext();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (nextKeyRecord == TERMINATE_VALUE){
            throw new NoSuchElementException();
        }

        return nextKeyRecord;

    }

    private AeroKeyRecord takeNext() throws InterruptedException {
        nextKeyRecord = queue.take();
        return nextKeyRecord;
    }

}
