/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import com.google.common.base.Preconditions;
import org.apache.giraph.graph.WorkerContext;
import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.giraph.examples.LongArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class GlobalClusteringCoefficientVertex extends
        EdgeListVertex<LongWritable, DoubleWritable,
        FloatWritable, LongArrayWritable> implements Tool {
    /** Configuration */
    private Configuration conf;
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(GlobalClusteringCoefficientVertex.class);
    /** The shortest paths id */
    public static String SOURCE_ID = "GlobalClusteringCoefficientVertex.sourceId";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;

    private List<String> triangles = new ArrayList<String>();

    /**
     * Is this vertex the source id?
     *
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() ==
            getContext().getConfiguration().getLong(SOURCE_ID,
                                                    SOURCE_ID_DEFAULT));
    }

    @Override
    public void compute(Iterator<LongArrayWritable> msgIterator) {
	DoubleSumAggregator trianglesum = (DoubleSumAggregator) getAggregator("trianglesum");
	DoubleSumAggregator triples = (DoubleSumAggregator) getAggregator("triples");
	if (getSuperstep() == 0) {
	    // Each vertex this is connected to

	    List<LongWritable> verticesl = new ArrayList<LongWritable>();
	    
	    // This is so we know which vertex the messages came from
	    verticesl.add(getVertexId());

	    // Find all connected vertices with ID less than current vertex
	    for (LongWritable targetVertexId : this) {
		if (targetVertexId.get() < getVertexId().get()) {
		    verticesl.add(targetVertexId);
		}
	    }

	    // Need to send list to other vertices, must convert to arraywritable
	    LongWritable[] verticesa = verticesl.toArray(new LongWritable[0]);
	    LongArrayWritable vertices = new LongArrayWritable(verticesa);
	    
	    // Sends list of smaller ID vertices to bigger ID vertices
	    for (LongWritable targetVertexId : this) {
		if (targetVertexId.get() > getVertexId().get()) {
		    sendMsg(targetVertexId, vertices);
		}
	    }
	} else if (getSuperstep() == 1) {
	    while (msgIterator.hasNext()) {
		LongArrayWritable law = msgIterator.next();
		Writable[] vertices = law.get();
		LongWritable source = (LongWritable) vertices[0];

		for (int i = 1; i < vertices.length; i++) {
		    if (hasEdge((LongWritable) vertices[i])) {
			double num = getVertexValue().get();
			setVertexValue(new DoubleWritable(1.0 + num));

			LongWritable[] one = new LongWritable[] { new LongWritable(1) };
			LongArrayWritable inc = new LongArrayWritable(one);

			sendMsg(source, inc);
			sendMsg(((LongWritable) vertices[i]), inc);
			
			triangles.add(source.toString());
			triangles.add(vertices[i].toString());
		    }
		}
	    }
	} else if (getSuperstep() == 2) {
	    while (msgIterator.hasNext()) {
		LongArrayWritable law = msgIterator.next();
		Writable[] msg = law.get();
		LongWritable value = (LongWritable) msg[0];

		double num = getVertexValue().get();
		setVertexValue(new DoubleWritable(num + value.get()));
	    }

	    trianglesum.aggregate(getVertexValue());

	    double sum = 0.0;
	    for (LongWritable source : this) {
		for (LongWritable target : this) {
		    if (source.get() > target.get()) {
			sum++;
		    }
		}
	    }

	    triples.aggregate(new DoubleWritable(sum));
	} else {
	    setVertexValue(new DoubleWritable(trianglesum.getAggregatedValue().get() / triples.getAggregatedValue().get()));

	    voteToHalt();
	}
    }

    public static class GlobalClusteringCoefficientContext extends WorkerContext {

	// Final sum value for verification for local jobs
	private static double FINAL_SUM_TRIANGLES, FINAL_SUM_TRIPLES;

	public static double getFinalSum() {
	    return FINAL_SUM_TRIANGLES/FINAL_SUM_TRIPLES;
	}

	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
	    registerAggregator("trianglesum", DoubleSumAggregator.class);
	    registerAggregator("triples", DoubleSumAggregator.class);
	}

	@Override
	public void postApplication() {
	    DoubleSumAggregator trianglesum = (DoubleSumAggregator) getAggregator("trianglesum");
	    DoubleSumAggregator triples = (DoubleSumAggregator) getAggregator("triples");

	    FINAL_SUM_TRIANGLES = trianglesum.getAggregatedValue().get();
	    FINAL_SUM_TRIPLES = triples.getAggregatedValue().get();
	}

	@Override
	public void preSuperstep() {
	    DoubleSumAggregator trianglesum = (DoubleSumAggregator) getAggregator("trianglesum");
	    DoubleSumAggregator triples = (DoubleSumAggregator) getAggregator("triples");

	    useAggregator("trianglesum");
	    useAggregator("triples");

	    if (getSuperstep() == 0) {
		trianglesum.setAggregatedValue(new DoubleWritable(0L));
		triples.setAggregatedValue(new DoubleWritable(0L));
	    }
	}

	@Override
	public void postSuperstep() { }
    }

    /**
     * VertexInputFormat that supports {@link GlobalClusteringCoefficientVertex}
     */
    public static class GlobalClusteringCoefficientVertexInputFormat extends
            TextVertexInputFormat<LongWritable,
                                  DoubleWritable,
                                  FloatWritable,
                                  DoubleWritable> {
        @Override
        public VertexReader<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new GlobalClusteringCoefficientVertexReader(
                textInputFormat.createRecordReader(split, context));
        }
    }

    /**
     * VertexReader that supports {@link GlobalClusteringCoefficientVertex}.  In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
    public static class GlobalClusteringCoefficientVertexReader extends
            TextVertexReader<LongWritable,
                DoubleWritable, FloatWritable, DoubleWritable> {

        public GlobalClusteringCoefficientVertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public BasicVertex<LongWritable, DoubleWritable, FloatWritable,
                           DoubleWritable> getCurrentVertex()
            throws IOException, InterruptedException {
          BasicVertex<LongWritable, DoubleWritable, FloatWritable,
              DoubleWritable> vertex = BspUtils.<LongWritable, DoubleWritable, FloatWritable,
                  DoubleWritable>createVertex(getContext().getConfiguration());

            Text line = getRecordReader().getCurrentValue();
            try {
                JSONArray jsonVertex = new JSONArray(line.toString());
                LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
                DoubleWritable vertexValue = new DoubleWritable(jsonVertex.getDouble(1));
                Map<LongWritable, FloatWritable> edges = Maps.newHashMap();
                JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
                for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                    JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                    edges.put(new LongWritable(jsonEdge.getLong(0)),
                            new FloatWritable((float) jsonEdge.getDouble(1)));
                }
                vertex.initialize(vertexId, vertexValue, edges, null);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Couldn't get vertex from line " + line, e);
            }
            return vertex;
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }
    }

    /**
     * VertexOutputFormat that supports {@link GlobalClusteringCoefficientVertex}
     */
    public static class GlobalClusteringCoefficientVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleWritable,
            FloatWritable> {

        @Override
        public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
                createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new GlobalClusteringCoefficientVertexWriter(recordWriter);
        }
    }

    /**
     * VertexWriter that supports {@link GlobalClusteringCoefficientVertex}
     */
    public static class GlobalClusteringCoefficientVertexWriter extends
            TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {
        public GlobalClusteringCoefficientVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<LongWritable, DoubleWritable,
                                FloatWritable, ?> vertex)
                throws IOException, InterruptedException {
            JSONArray jsonVertex = new JSONArray();
            try {
                jsonVertex.put(vertex.getVertexId().get());
                jsonVertex.put(vertex.getVertexValue().get());
                JSONArray jsonEdgeArray = new JSONArray();
                for (LongWritable targetVertexId : vertex) {
                    JSONArray jsonEdge = new JSONArray();
                    jsonEdge.put(targetVertexId.get());
                    jsonEdge.put(vertex.getEdgeValue(targetVertexId).get());
                    jsonEdgeArray.put(jsonEdge);
                }
                jsonVertex.put(jsonEdgeArray);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "writeVertex: Couldn't write vertex " + vertex);
            }
            getRecordWriter().write(new Text(jsonVertex.toString()), null);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] argArray) throws Exception {
        Preconditions.checkArgument(argArray.length == 4,
            "run: Must have 4 arguments <input path> <output path> " +
            "<source vertex id> <# of workers>");

        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.setVertexClass(getClass());
        job.setVertexInputFormatClass(
            GlobalClusteringCoefficientVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            GlobalClusteringCoefficientVertexOutputFormat.class);
	job.setWorkerContextClass(GlobalClusteringCoefficientContext.class);
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        job.getConfiguration().setLong(GlobalClusteringCoefficientVertex.SOURCE_ID,
                                       Long.parseLong(argArray[2]));
        job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
                                   Integer.parseInt(argArray[3]),
                                   100.0f);

        return job.run(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GlobalClusteringCoefficientVertex(), args));
    }
}
