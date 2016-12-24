package com.manju.hadoopproject.anagram;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer  extends Reducer<Text,Text,NullWritable,Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for (Text val : values) {
			sb.append(val.toString() + ",");
			count++;
		}
		if(count > 1) {
			context.write(NullWritable.get(), new Text(sb.toString().substring(0, sb.length()-1)));
		}
	}
}
