package com.manju.hadoopproject.anagram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * @Author : Manjunath Kempaiah
 * @Version :  1.0	
 * 
 * <p>Find anagrams in a huge text. An anagram is basically a different arrangement of letters in a word. Anagram does not need to be meaningful.
 * Input Values:
 *     “the cat act in tic tac toe.”
 * Output Values:
 * 		cat, tac, act
 * </p>
 */
public class AnagramDriver {
    public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram");
        job.setJarByClass(AnagramDriver.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(conf);
        boolean isOutputDirExists = fs.exists(new Path(args[1]));
        if(isOutputDirExists){ 
        	fs.delete(new Path(args[1]), true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
