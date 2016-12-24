package com.manju.hadoopproject.letterfrequencey;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	
	private Text character = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	    	  String word = itr.nextToken();
	    	  for(int i=0; i < word.length(); i++) {
	    		  char temp = word.charAt(i);
	    		  if((temp >= 65 && temp <= 90) || (temp >= 97 && temp <= 122)) {
	    			  character.set(String.valueOf(temp));	
	    			  context.write(character, one);
	    		  }
	    	  }	        
	      }
	}
}
