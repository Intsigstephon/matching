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

import org.apache.giraph.graph.DefaultVertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Contains the logic for computing bids and prices at each node in the bipartite graph at each
 * step in the computation. The implementation of the {@code compute} method follows Bertsekas' auction
 * algorithm.
 * 
 * @see <a href="http://18.7.29.232/bitstream/handle/1721.1/3154/P-1908-20783037.pdf?sequence=1">Algorithm Tutorial</a>
 */
public class BipartiteMatchingVertex extends DefaultVertex<Text, VertexState, IntWritable> {
}
