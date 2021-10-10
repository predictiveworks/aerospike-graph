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
    public AeroRecordReader(AeroSplit split) throws IOException {
        log.info("Construct Aerospike record reader from the old API.");
        prepare(split);
    }

    /** INTERNAL METHODS **/

    private void prepare(AeroSplit split) throws IOException {
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

    /** INTERFACE METHODS **/

    @Override
    public boolean next(AeroKey aeroKey, AeroRecord aeroRecord) throws IOException {
        return false;
    }

    @Override
    public AeroKey createKey() {
        return null;
    }

    @Override
    public AeroRecord createValue() {
        return null;
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
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
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
