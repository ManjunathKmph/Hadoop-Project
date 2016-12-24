package com.manju.hadoopproject.dnamirror;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DNAMirrorMapper extends Mapper<Object, Text, DNAMirrorKey, Text> {
	
	private Text dna = new Text();
	
	private Text people = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String[] str = value.toString().split(" ");
		dna.set(str[1]);
		DNAMirrorKey customKey = new DNAMirrorKey(dna);
		people.set(str[0]);
		context.write(customKey, people);
	}
}
