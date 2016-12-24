package com.manju.hadoopproject.dna;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DNAMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text dna = new Text();
	
	private Text people = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String[] str = value.toString().split(" ");
		dna.set(str[1]);
		people.set(str[0]);
		context.write(dna, people);
	}
}
