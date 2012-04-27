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
import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
/*import org.apache.giraph.examples.MaxAggregator;
import org.apache.giraph.examples.MinAggregator;
import org.apache.giraph.examples.LongSumAggregator;*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
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

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class PageRank extends
        EdgeListVertex<LongWritable, DoubleWritable,
        FloatWritable, DoubleWritable> implements Tool {
    /** Configuration */
    private Configuration conf;
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(PageRank.class);
    /** The shortest paths id */
    public static String SOURCE_ID = "PageRank.sourceId";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;

	private static final int MAX_SUPERSTEPS = 30;

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
    public void compute(Iterator<DoubleWritable> msgIterator) {
        LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator("sum");
        MinAggregator minAggreg = (MinAggregator) getAggregator("min");
        MaxAggregator maxAggreg = (MaxAggregator) getAggregator("max");
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            setVertexValue(vertexValue);
            maxAggreg.aggregate(vertexValue);
            minAggreg.aggregate(vertexValue);
            sumAggreg.aggregate(1L);
            LOG.info(getVertexId() + ": PageRank=" + vertexValue +
                     " max=" + maxAggreg.getAggregatedValue() +
                     " min=" + minAggreg.getAggregatedValue());
        }

        if (getSuperstep() < MAX_SUPERSTEPS) {
            long edges = getNumOutEdges();
            sendMsgToAllEdges(
                new DoubleWritable(getVertexValue().get() / edges));
        } else {
//setVertexValue(maxAggreg.getAggregatedValue());
            voteToHalt();
        }
    }

	public static class HardRankWorkerContext extends
    		WorkerContext {

    	public static double finalMax, finalMin;
    	public static long finalSum;
    	
    	@Override
    	public void preApplication() 
    	throws InstantiationException, IllegalAccessException {
    		
    		registerAggregator("sum", LongSumAggregator.class);
    		registerAggregator("min", MinAggregator.class);
    		registerAggregator("max", MaxAggregator.class);			
    	}

    	@Override
    	public void postApplication() {

    		LongSumAggregator sumAggreg = 
    			(LongSumAggregator) getAggregator("sum");
    		MinAggregator minAggreg = 
    			(MinAggregator) getAggregator("min");
    		MaxAggregator maxAggreg = 
    			(MaxAggregator) getAggregator("max");

    		finalSum = sumAggreg.getAggregatedValue().get();
    		finalMax = maxAggreg.getAggregatedValue().get();
    		finalMin = minAggreg.getAggregatedValue().get();
    		
            LOG.info("aggregatedNumVertices=" + finalSum);
            LOG.info("aggregatedMaxPageRank=" + finalMax);
            LOG.info("aggregatedMinPageRank=" + finalMin);
    	}

		@Override
		public void preSuperstep() {
    		
	        LongSumAggregator sumAggreg = 
	        	(LongSumAggregator) getAggregator("sum");
	        MinAggregator minAggreg = 
	        	(MinAggregator) getAggregator("min");
	        MaxAggregator maxAggreg = 
	        	(MaxAggregator) getAggregator("max");
	        
	        if (getSuperstep() >= 3) {
	            LOG.info("aggregatedNumVertices=" +
	                    sumAggreg.getAggregatedValue() +
	                    " NumVertices=" + getNumVertices());
	            if (sumAggreg.getAggregatedValue().get() != getNumVertices()) {
	                throw new RuntimeException("wrong value of SumAggreg: " +
	                        sumAggreg.getAggregatedValue() + ", should be: " +
	                        getNumVertices());
	            }
	            DoubleWritable maxPagerank =
	                    (DoubleWritable) maxAggreg.getAggregatedValue();
	            LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
	            DoubleWritable minPagerank =
	                    (DoubleWritable) minAggreg.getAggregatedValue();
	            LOG.info("aggregatedMinPageRank=" + minPagerank.get());
	        }
	        useAggregator("sum");
	        useAggregator("min");
	        useAggregator("max");
	        sumAggreg.setAggregatedValue(new LongWritable(0L));
		}

		@Override
		public void postSuperstep() { }
    }

    /**
     * VertexInputFormat that supports {@link SimpleShortestPathsVertex}
     */
    public static class PageRankInputFormat extends
            TextVertexInputFormat<LongWritable,
                                  DoubleWritable,
                                  FloatWritable,
                                  DoubleWritable> {
        @Override
        public VertexReader<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new PageRankReader(
                textInputFormat.createRecordReader(split, context));
        }
    }

    /**
     * VertexReader that supports {@link SimpleShortestPathsVertex}.  In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
    public static class PageRankReader extends
            TextVertexReader<LongWritable,
                DoubleWritable, FloatWritable, DoubleWritable> {

        public PageRankReader(
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
     * VertexOutputFormat that supports {@link SimpleShortestPathsVertex}
     */
    public static class PageRankOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleWritable,
            FloatWritable> {

        @Override
        public VertexWriter<LongWritable, DoubleWritable, FloatWritable>
                createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new PageRankWriter(recordWriter);
        }
    }

    /**
     * VertexWriter that supports {@link SimpleShortestPathsVertex}
     */
    public static class PageRankWriter extends
            TextVertexWriter<LongWritable, DoubleWritable, FloatWritable> {
        public PageRankWriter(
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
            PageRankInputFormat.class);
        job.setVertexOutputFormatClass(
            PageRankOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        job.getConfiguration().setLong(PageRank.SOURCE_ID,
                                       Long.parseLong(argArray[2]));
        job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
                                   Integer.parseInt(argArray[3]),
                                   100.0f);
	job.setWorkerContextClass(HardRankWorkerContext.class);

        return job.run(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PageRank(), args));
    }
}
