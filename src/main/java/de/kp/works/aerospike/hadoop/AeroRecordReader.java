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
import com.aerospike.client.Record;
import de.kp.works.aerospike.util.NamedThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

public class AeroRecordReader extends
        RecordReader<AeroKey, AeroRecord> implements
        org.apache.hadoop.mapred.RecordReader<AeroKey, AeroRecord> {

    private static final Log log = LogFactory
            .getLog(AeroRecordReader.class);

    private AeroKey currentKey;
    private AeroRecord currentValue;

    private Iterator<AeroKeyRecord> scanIterator;
    private float progress = 0f;

    /** OLD HADOOP API
     *
     * This API is not used by [NewHadoopRDD]
     */
    public AeroRecordReader() {
        log.info("Construct Aerospike record reader from old API.");
    }

    /** NEW HADOOP API
     *
     * This API is used by [NewHadoopRDD]
     */
    public AeroRecordReader(AeroSplit split) throws Exception {
        log.info("Construct Aerospike record reader from the old API.");
        prepare(split);
    }

    /** INTERNAL METHODS **/

    private void prepare(AeroSplit split) throws Exception {
        /*
         * Build named [ThreadFactory] for this split
         */
        String threadGroup = "aero-hadoop" + split.getNode();
        String scanPrefix = "scan-" + split.getNode();

        NamedThreadFactory scanThreadFactory = new NamedThreadFactory(threadGroup, scanPrefix);
        /*
         * Build Aerospike scan reader and start scanning
         * the provided node
         */
        AeroScanReader scanReader = new AeroScanReader(split.getNode(), split.getConfig());
        scanIterator = scanReader.run(scanThreadFactory);

    }

    private AeroKey setCurrentKey(AeroKey currentKey, Key key) {

        if (currentKey == null) {
            currentKey = new AeroKey(key);
        }

        return currentKey;

    }

    private AeroRecord setCurrentValue(AeroRecord currentValue, Record record) {

        if (currentValue == null) {
            currentValue = new AeroRecord(record);
        }

        return currentValue;
    }

    /** INTERFACE METHODS **/

    @Override
    public AeroKey createKey() {
        return new AeroKey();
    }

    @Override
    public AeroRecord createValue() {
        return new AeroRecord();
    }

    @Override
    public long getPos() {
        return 0;
    }

    @Override
    public boolean next(AeroKey aeroKey, AeroRecord aeroRecord) throws IOException {

        try {

            boolean hasNext = scanIterator.hasNext();
            if (hasNext) {
                /*
                 * Retrieve next entry from Scan iterator
                 */
                AeroKeyRecord entry = scanIterator.next();
                if (entry == null)
                    return false;
                else {
                    AeroKey nextKey = entry.key;
                    AeroRecord nextValue = entry.rec;

                    currentKey = setCurrentKey(currentKey, nextKey.toKey());
                    currentValue = setCurrentValue(currentValue, nextValue.toRecord());

                }
            } else {
                progress = 1f;
                return false;
            }
        } catch (Exception e) {
            throw new IOException(String.format("Reading next (key, value) failed: %s", e.getLocalizedMessage()));

        }

        return false;

    }
    /**
     * This method is used by [NewHadoopRDD]
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        try {
            prepare((AeroSplit) inputSplit);

        } catch (Exception e) {
            /* Do nothing */
        }
    }
    /**
     * This method is used by [NewHadoopRDD]
     * to retrieve key value pairs
     */
    @Override
    public boolean nextKeyValue() throws IOException {

        if (currentKey == null) {
            currentKey = createKey();
        }

        if (currentValue == null) {
            currentValue = createValue();
        }
        /*
         * Delegate request to `next` interface method
         */
        return next(currentKey, currentValue);
    }

    @Override
    public AeroKey getCurrentKey() {
        return currentKey;
    }

    @Override
    public AeroRecord getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() {
        return progress;
    }

    @Override
    public void close() throws IOException {
        /*
         * Do nothing to do, as the Scan iterator
         * automatically closes the thread.
         */
    }
}
