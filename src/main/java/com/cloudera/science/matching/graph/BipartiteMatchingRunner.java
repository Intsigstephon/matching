/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.matching.graph;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main class that runs the Giraph implementation of the auction algorithm for
 * solving assignment problems.
 * 
 */
public class BipartiteMatchingRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: <input> <output> <numworkers>");
      System.err.println("The input should be the output of the InputPreparer Crunch pipeline.");
      System.err.println("The output is the directory where the output of the matching will be");
      System.err.println("written, and the numworkers should be <= the number of map slots available");
      System.err.println("on your Hadoop cluster.");
      return 1;
    }

    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    GiraphConfiguration conf = job.getConfiguration();
    conf.setVertexClass(BipartiteMatchingVertex.class);
    conf.setVertexInputFormatClass(BipartiteMatchingVertexInputFormat.class);
    conf.setVertexOutputFormatClass(BipartiteMatchingVertexOutputFormat.class);
    conf.setComputationClass(BipartiteMatching.class);
    conf.setOutEdgesClass(ArrayListEdges.class);
    GiraphFileInputFormat.addVertexInputPath(job.getConfiguration(), new Path(args[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
    
    int numWorkers = Integer.parseInt(args[2]);
    job.getConfiguration().setWorkerConfiguration(numWorkers, numWorkers, 100.0f);
    
    return job.run(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BipartiteMatchingRunner(), args);
  }
}
