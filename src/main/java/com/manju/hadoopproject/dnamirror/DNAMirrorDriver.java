package com.manju.hadoopproject.dnamirror;

import org.apache.hadoop.conf.Configuration;
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
 * <p>A file contains the DNA sequence of people. Find all the people who have same or mirror image of DNAs.
 * Input Values:
 *      “User1 ACGT” 
 *		“User2 TGCA”
 *		“User3 ACG”
 *		“User4 ACGT”
 *		“User5 ACG”
 *		“User6 AGCT”
 * Output Values:
 *		User1, User2, User4
 *		User3, User 5
 * </p>
 */
public class DNAMirrorDriver {
    public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DNA Mirror");
        job.setJarByClass(DNAMirrorDriver.class);
        job.setMapperClass(DNAMirrorMapper.class);
        job.setReducerClass(DNAMirrorReducer.class);
        job.setMapOutputKeyClass(DNAMirrorKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
