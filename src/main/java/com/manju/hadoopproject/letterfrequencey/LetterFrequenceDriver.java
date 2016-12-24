package com.manju.hadoopproject.letterfrequencey;

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
 * <p>To find frequencies of letters [a-z] in a huge text of few GBs.
 * Input Values:
 *     You give any file which has text values as input.
 * Output Values:
 * 		It will print for each leter how many times it has appeared in the file.
 * </p>
 */
public class LetterFrequenceDriver {
    public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Letter Frquencey");
        job.setJarByClass(LetterFrequenceDriver.class);
        job.setMapperClass(LetterFrequencyMapper.class);
        job.setReducerClass(LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
