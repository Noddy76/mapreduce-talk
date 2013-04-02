package org.queeg.talks.mapreduce;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import static com.google.common.collect.Sets.*;

public class PlaysReachReducer extends Reducer<IntWritable, IntWritable, IntWritable, PlaysReach> {
  private PlaysReach playsReach = new PlaysReach();

  @Override
  protected void reduce(IntWritable track, Iterable<IntWritable> users, Context context) throws IOException,
      InterruptedException {
    int plays = 0;
    Set<Integer> uniqueUsers = newHashSet();

    for (IntWritable user : users) {
      plays++;
      uniqueUsers.add(user.get());
    }

    playsReach.setPlays(plays);
    playsReach.setReach(uniqueUsers.size());

    context.write(track, playsReach);
  }
}
