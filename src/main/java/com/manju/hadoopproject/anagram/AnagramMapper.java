package com.manju.hadoopproject.anagram;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text sortedCharacters = new Text();
	
	private Text originalCharacters = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	    	  String word = itr.nextToken();
	    	  char[] chars = word.toCharArray();
	    	  Arrays.sort(chars);
	    	  sortedCharacters.set(new String(chars));
	    	  originalCharacters.set(word);
	    	  context.write(sortedCharacters, originalCharacters);
	      }
	}
}
