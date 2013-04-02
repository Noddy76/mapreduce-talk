package org.queeg.talks.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PlaysReachJob extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new PlaysReachJob(), args);

    if (status != 0) {
      System.exit(status);
    }
  }

  public int run(String[] args) throws Exception {
    String jobName = "PlaysReach";
    Job job = Job.getInstance(getConf(), jobName);
    job.setJarByClass(getClass());

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(PlaysReachMapper.class);
    job.setReducerClass(PlaysReachReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PlaysReach.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
