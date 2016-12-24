package com.manju.hadoopproject.dna;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DNAReducer  extends Reducer<Text,Text,NullWritable,Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for (Text val : values) {
			sb.append(val.toString() + ",");
		}
		context.write(NullWritable.get(), new Text(sb.toString().substring(0, sb.length()-1)));
	}
}
