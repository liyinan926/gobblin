package gobblin.runtime.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import gobblin.source.workunit.ImmutableWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * A custom {@link org.apache.hadoop.mapreduce.InputSplit} that corresponds to a Gobblin
 * {@link gobblin.source.workunit.WorkUnit}.
 *
 * @author ynli
 */
public class GobblinInputSplit extends InputSplit implements Writable {

  private final WorkUnit workUnit;

  public GobblinInputSplit(WorkUnit workUnit) {
    this.workUnit = new ImmutableWorkUnit(workUnit);
  }

  @Override
  public long getLength()
      throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations()
      throws IOException, InterruptedException {
    return new String[0];
  }

  /**
   * Get the Gobblin {@link gobblin.source.workunit.WorkUnit} wrapped by this
   * {@link GobblinInputSplit}.
   *
   * @return a Gobblin {@link gobblin.source.workunit.WorkUnit}
   */
  public WorkUnit getWorkUnit() {
    return this.workUnit;
  }

  @Override
  public void write(DataOutput dataOutput)
      throws IOException {
    this.workUnit.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput)
      throws IOException {
    this.workUnit.readFields(dataInput);
  }
}
