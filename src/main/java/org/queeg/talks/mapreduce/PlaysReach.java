package org.queeg.talks.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PlaysReach implements Writable {
  private long plays;
  private int reach;

  public PlaysReach() {
  }

  public PlaysReach(PlaysReach other) {
    this.plays = other.plays;
    this.reach = other.reach;
  }

  public long getPlays() {
    return plays;
  }

  public void setPlays(long plays) {
    this.plays = plays;
  }

  public int getReach() {
    return reach;
  }

  public void setReach(int reach) {
    this.reach = reach;
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(plays);
    out.writeInt(reach);
  }

  public void readFields(DataInput in) throws IOException {
    plays = in.readLong();
    reach = in.readInt();
  }
  
  @Override
  public String toString() {
    return plays + "\t" + reach;
  }
}
