package gobblin.runtime.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.SchemaConversionException;
import gobblin.runtime.MultiConverter;
import gobblin.runtime.TaskContext;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;


/**
 * A custom {@link org.apache.hadoop.mapreduce.RecordReader} that wraps a Gobblin
 * {@link gobblin.source.extractor.Extractor}.
 *
 * @author ynli
 */
public class GobblinRecordReader<K, V> extends RecordReader<K, V> {

  private Extractor<K, V> extractor;
  private Object schema;
  private V record;
  private long recordsRead = 0;

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    WorkUnitState workUnitState = new WorkUnitState(((GobblinInputSplit) inputSplit).getWorkUnit());
    TaskContext taskContext = new TaskContext(workUnitState);
    this.extractor = taskContext.getSource().getExtractor(workUnitState);
    this.schema = this.extractor.getSchema();
    Converter converter = new MultiConverter(taskContext.getConverters());
    try {
      this.schema = converter.convertSchema(this.schema, workUnitState);
    } catch (SchemaConversionException sce) {
      throw new IOException(sce);
    }
  }

  @Override
  public boolean nextKeyValue()
      throws IOException, InterruptedException {
    try {
      this.record = this.extractor.readRecord(this.record);
      if (this.record != null) {
        this.recordsRead++;
      }
      return this.record != null;
    } catch (DataRecordException dre) {
      throw new IOException(dre);
    }
  }

  @Override
  public K getCurrentKey()
      throws IOException, InterruptedException {
    return null;
  }

  @Override
  public V getCurrentValue()
      throws IOException, InterruptedException {
    return this.record;
  }

  @Override
  public float getProgress()
      throws IOException, InterruptedException {
    if (this.record == null) {
      return 100f;
    }

    long expectedCount = this.extractor.getExpectedRecordCount();
    return this.recordsRead < expectedCount ? (float) this.recordsRead / expectedCount : 100f;
  }

  @Override
  public void close()
      throws IOException {
    this.extractor.close();
  }

  /**
   * Get the (converted) schema of the extracted data.
   *
   * @return (converted) schema of the extracted data
   */
  public Object getSchema() {
    return this.schema;
  }
}
