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

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.science.matching.VertexData;


/**
 * OutputFormat for BipartiteMatchingVertex.
 */
public class BipartiteMatchingVertexOutputFormat extends
        TextVertexOutputFormat<Text, VertexState, IntWritable> {

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new BipartiteMatchingVertexWriter();
  }

  private static final Text BLANK = new Text("");

  public class BipartiteMatchingVertexWriter extends TextVertexWriter {

    private ObjectMapper mapper;
    private RecordWriter<Text, Text> writer;

    public BipartiteMatchingVertexWriter() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
      this.writer = createLineRecordWriter(context);
    }

    @Override
    public void writeVertex(Vertex<Text, VertexState, IntWritable> bmv)
        throws IOException, InterruptedException {
      VertexData vertexData = new VertexData(bmv.getId(), bmv.getValue(), bmv.getEdges());
      writer.write(BLANK, new Text(mapper.writeValueAsString(vertexData)));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      this.writer.close(context);
    }
  }
}
