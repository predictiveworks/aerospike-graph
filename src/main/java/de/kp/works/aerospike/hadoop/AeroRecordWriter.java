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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class AeroRecordWriter extends
        RecordWriter<AeroKey, AeroRecord> implements
        org.apache.hadoop.mapred.RecordWriter<AeroKey, AeroRecord> {

    private static final Log log = LogFactory
            .getLog(AeroRecordWriter.class);

    protected final Configuration cfg;

    @SuppressWarnings("unused")
    private Progressable progressable;
    /**
     * Flag to indicate whether the connection to
     * the Aerospike database was initialized
     */
    protected boolean initialized = false;

    public AeroRecordWriter(Configuration cfg, Progressable progressable) {
        this.cfg = cfg;
        this.progressable = progressable;
    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }

    @Override
    public void write(AeroKey aeroKey, AeroRecord aeroRecord) throws IOException {

        if (!initialized) {
            initialized = initialize();
        }

    }

    /** HELPER METHODS **/

    protected Boolean initialize() throws IOException {
        return false;
    }

}
