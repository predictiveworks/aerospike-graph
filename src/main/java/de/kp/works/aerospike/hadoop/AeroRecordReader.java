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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class AeroRecordReader extends
        RecordReader<AeroKey, AeroRecord> implements
        org.apache.hadoop.mapred.RecordReader<AeroKey, AeroRecord> {

    private static final Log log = LogFactory
            .getLog(AeroRecordReader.class);

    /** OLD HADOOP API
     *
     * This API is not used by [NewHadoopRDD]
     */
    public AeroRecordReader() {
        log.info("Construct Aerospike record reader from old API.");
    }

    public AeroRecordReader(AeroSplit split) throws IOException {
        log.info("Construct Aerospike record reader from the old API.");
        prepare(split);
    }

    /** INTERNAL METHODS **/

    private void prepare(AeroSplit split) throws IOException {
        // TODO
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
    public AeroKey getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public AeroRecord getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
