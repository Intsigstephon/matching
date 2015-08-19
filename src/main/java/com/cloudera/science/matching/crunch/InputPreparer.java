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

import static org.apache.crunch.types.avro.Avros.*;

import java.util.List;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.science.matching.VertexData;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class InputPreparer extends Configured implements Tool {

  /**
   * Map task that splits a single line of delimited text that consists of ID1,ID2,WEIGHT into the data
   * that is fed into the reduce task that aggregates the information about each ID into a single
   * record.
   */
  public static class TwoVerticesFn extends DoFn<String, Pair<String, Pair<String, Integer>>> {
    private final String sep;
    
    public TwoVerticesFn(String sep) {
      this.sep = sep;
    }
    
    @Override
    public void process(String input, Emitter<Pair<String, Pair<String, Integer>>> emitter) {
      List<String> pieces = Lists.newArrayList(Splitter.on(sep).split(input));
      String id1 = pieces.get(0);
      String id2 = pieces.get(1);
      Integer score = Integer.valueOf(pieces.get(2));
      if (!id1.equals(id2) && score >= 0) {
        emitter.emit(Pair.of(id1, Pair.of(id2, score)));
        emitter.emit(Pair.of(id2, Pair.of(id1, -1)));
      }
    }
  }
  
  /**
   * Reduce task that aggregates the data about each bidder/object vertex and checks for error
   * conditions, like an identifier that is both a bidder and an object.
   */
  public static class WriteVertexFn extends MapFn<Pair<String, Iterable<Pair<String, Integer>>>, VertexData> {
    @Override
    public VertexData map(Pair<String, Iterable<Pair<String, Integer>>> v) {
      List<Pair<String, Integer>> pairs = Lists.newArrayList(v.second());
      Map<String, Integer> targets = Maps.newHashMap();
      boolean bidder = true;
      for (int i = 0; i < pairs.size(); i++) {
        String id = pairs.get(i).first();
        Integer score = pairs.get(i).second();
        if (i == 0) {
          if (score < 0) {
            bidder = false;
          }
        } else if (bidder && score < 0) {
          throw new IllegalStateException(
              String.format("Invalid input: vertex id %s occurs in both sides of the graph", id));
        } else if (!bidder && score >= 0) {
          throw new IllegalStateException(
              String.format("Invalid input: vertex id %s occurs in both sides of the graph", id));
        }
        targets.put(id, score);
      }
      return new VertexData(v.first(), bidder, targets);
    }
  }
  
  public PCollection<VertexData> exec(PCollection<String> input, String sep) {
    return input
        .parallelDo(new TwoVerticesFn(sep), tableOf(strings(), pairs(strings(), ints())))
        .groupByKey()
        .parallelDo(new WriteVertexFn(), PTypes.jsonString(VertexData.class, WritableTypeFamily.getInstance()));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: <input> <output> <delim>");
      System.err.println("The input should be a file with 3-columns: bidder ID, object ID, and weight,");
      System.err.println("separated by the given delimiter.");
      System.err.println("The output will be a text file of JSON-serialized information about each vertex");
      System.err.println("that is the input to the BipartiteMatchingRunner.");
      return 1;
    }
    
    Pipeline p = new MRPipeline(InputPreparer.class, getConf());
    PCollection<VertexData> data = exec(p.read(From.textFile(args[0])), args[2]);
    data.write(To.textFile(args[1]));
    p.done();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InputPreparer(), args);
  }
}
