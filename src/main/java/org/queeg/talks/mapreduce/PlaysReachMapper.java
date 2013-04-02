package org.queeg.talks.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;

public class PlaysReachMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
  private Splitter recordSplitter = Splitter.on(" ");

  private IntWritable userWritable = new IntWritable();
  private IntWritable trackWritable = new IntWritable();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Iterator<String> recordIterator = recordSplitter.split(value.toString()).iterator();

    recordIterator.next(); // Date
    int user = Integer.parseInt(recordIterator.next());
    recordIterator.next(); // Album
    recordIterator.next(); // Artist
    int track = Integer.parseInt(recordIterator.next());
    
    userWritable.set(user);
    trackWritable.set(track);

    context.write(trackWritable, userWritable);
  }
}
