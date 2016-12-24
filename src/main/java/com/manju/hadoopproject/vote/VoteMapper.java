package com.manju.hadoopproject.vote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VoteMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Map<String, Integer> voterWorthMap = new HashMap<String, Integer>();
	
	private Text character = new Text();
	
	@Override
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("Calling setup method");
		try {
			URI[] voteWorthFiles = context.getCacheFiles();
			if(voteWorthFiles != null && voteWorthFiles.length > 0) {
				for(URI voteWorthFile : voteWorthFiles) {
					readFile(voteWorthFile);
				}
			}			
		} catch(IOException ex) {
			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
		super.setup(context);
	}

	private void readFile(URI filePath) throws IOException{ 
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(new File(filePath.toString())));
			String line = null;
			while((line = bufferedReader.readLine()) != null) {
				String[] str = line.split(" ");
				voterWorthMap.put(str[0], Integer.parseInt(str[1]));
			}
			bufferedReader.close();
		} finally {
			if(bufferedReader != null) {
				bufferedReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String[] str = value.toString().split(" ");
		for(int i=0; i < 2; i++) {
			if(i == 0) {
				character.set(str[1]);
				Integer worthValue = voterWorthMap.get(str[0]);
				context.write(character, new IntWritable(worthValue));
			} else {
				character.set(str[0]);
				context.write(character, new IntWritable(0));
			}
		}
	}
}
