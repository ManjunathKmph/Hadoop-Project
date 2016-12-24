package com.manju.hadoopproject.vote;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * @Author : Manjunath Kempaiah
 * @Version :  1.0	
 * 
 * <p>In an unusual democracy, everyone is not equal. The vote count is a function of worth of the voter. Though everyone is voting for each other.
 *	As example, if A with a worth of 5 and B with a worth of 1 are voting for C, the vote count of C would be 6.
 *	You are given a list of people with their value of vote.You are also given another list describing who voted for who all.
 * Input Values:
 *     List1               List2
 *		Voter Votee        Person Worth   
 *		A      C            A      5  
 *		B      C            B      1
 *		C      F            C      11
 * Output Values:
 *      Person     VoteCount		
 *        A           0
 *        B           0
 *        C           6
 *        F			  11
 * </p>
 */
public class VoteDriver {
	
	public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Vote");
        job.setJarByClass(VoteDriver.class);
        job.setMapperClass(VoteMapper.class);
        job.setReducerClass(VoteReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new URI(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
