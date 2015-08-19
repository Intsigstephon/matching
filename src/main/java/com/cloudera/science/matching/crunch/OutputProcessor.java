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
package com.cloudera.science.matching.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.science.matching.VertexData;

/**
 * Processes the output of the Giraph job and emits the matched pairs.
 */
public class OutputProcessor extends Configured implements Tool {

  private static final PType<VertexData> vType = PTypes.jsonString(VertexData.class, WritableTypeFamily.getInstance());
  
  /**
   * Extracts the ID1,ID2,WEIGHT values from the JSON data written by the Giraph job.
   * 
   * @param giraphOutput A {@code PCollection} that represents the output of the Giraph job.
   * @return the ID1,ID2,WEIGHT values for each line of output.
   */
  public static PCollection<String> exec(PCollection<VertexData> giraphOutput) {
    return giraphOutput.parallelDo(new DoFn<VertexData, String>() {
      @Override
      public void process(VertexData input, Emitter<String> emitter) {
        if (input.isBidder()) {
          String vertexId = input.getVertexId();
          String matchId = input.getMatchId();
          Integer score = input.getEdges().get(matchId);
          emitter.emit(String.format("%s,%s,%s", vertexId, matchId, score));
        }
      }
    }, Writables.strings());
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: <input> <output>");
      System.err.println("The input should be the output of the BipartiteMatchingRunner, and the output");
      System.err.println("of this job is a CSV file containing ID1,ID2,WEIGHT values for the matched");
      System.err.println("pairs of bidder/object vertices in the bipartite graph.");
      return 1;
    }
    Pipeline p = new MRPipeline(OutputProcessor.class, getConf());
    exec(p.read(From.textFile(args[0], vType))).write(To.textFile(args[1]));
    p.done();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new OutputProcessor(), args);
  }
}
