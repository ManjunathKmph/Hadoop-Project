package com.manju.hadoopproject.dna;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DNAReducer  extends Reducer<Text,Text,NullWritable, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		List<String> valuesList = new ArrayList<String>();
		for (Text val : values) {
			valuesList.add(val.toString());
		}
		Collections.sort(valuesList);
		for(String val : valuesList) {
			sb.append(val + ",");
		}
		context.write(NullWritable.get(), new Text(sb.toString().substring(0, sb.length()-1)));
	}
}
