package org.apache.giraph.examples;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] values) {
		super(LongWritable.class, values);
	}
}
