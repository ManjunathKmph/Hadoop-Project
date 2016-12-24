package com.manju.hadoopproject.dnamirror;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DNAMirrorReducer  extends Reducer<DNAMirrorKey,Text,NullWritable,Text> {
	
	public void reduce(DNAMirrorKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for (Text val : values) {
			sb.append(val.toString() + ",");
		}
		context.write(NullWritable.get(), new Text(sb.toString().substring(0, sb.length()-1)));
	}
}
