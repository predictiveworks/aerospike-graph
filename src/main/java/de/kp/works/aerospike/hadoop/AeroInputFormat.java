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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Node;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This class is built to work with the NewHadoopRDD
 * of Apache Spark.
 */
public class AeroInputFormat
        extends InputFormat<AeroKey, AeroRecord>
        implements org.apache.hadoop.mapred.InputFormat<AeroKey,AeroRecord> {

    private static final Log log =
            LogFactory.getLog(AeroInputFormat.class);
   /**
    * This method is also part of the new Hadoop API
    * leveraged by Apache Spark.
    *
    */
   @Override
   public RecordReader<AeroKey, AeroRecord> createRecordReader(
           InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
       return new AeroRecordReader((AeroSplit) inputSplit);
   }

   /**
    * This method supports the new Hadoop API leveraged by Apache Spark.
    * Is is used as a wrapper to delegate requests to the old API.
    */
   @Override
   public List<InputSplit> getSplits(JobContext jobContext) throws IOException {

        AeroConfig cfg = new AeroConfig(jobContext.getConfiguration());
        AerospikeClient client = AeroClient.getInstance(cfg);
        /*
         * Build input split with respect to the
         * existing Aerospike cluster nodes
         */
        Node[] nodes = client.getNodes();

        int numSplits = nodes.length;
        if (numSplits == 0) {
            throw new IOException("No Aerospike cluster nodes available.");
        }

        log.info(String.format("%d Aerospike cluster node(s) found.", numSplits));
        AeroSplit[] splits = new AeroSplit[numSplits];
        for (int i = 0; i < numSplits; i++) {

            Node node = nodes[i];
            String name = node.getName();
            Host host = node.getHost();

            splits[i] = new AeroSplit(name, host.name, host.port, cfg);

        }

        return Arrays.asList(splits);

    }

    /** OLD HADOOP API
     *
     * This API is not used by [NewHadoopRDD]
     */
    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        return new org.apache.hadoop.mapred.InputSplit[0];
    }

    @Override
    public org.apache.hadoop.mapred.RecordReader<AeroKey, AeroRecord> getRecordReader(
            org.apache.hadoop.mapred.InputSplit inputSplit, JobConf jobConf, Reporter reporter) {
        /*
         * Dummy constructor to satisfy the requirements
         * of the old Hadoop API.
         */
        return new AeroRecordReader();
    }

}
