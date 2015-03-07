package gobblin.runtime.spark;

import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;

import gobblin.writer.DataWriter;


/**
 * A Spark {@link org.apache.spark.api.java.function.VoidFunction} that wraps a Gobblin
 * {@link gobblin.writer.DataWriter}.
 *
 * @author ynli
 */
public class DataWriterFunction implements VoidFunction<Iterator<Object>> {

  private final DataWriter dataWriter;

  public DataWriterFunction(DataWriter<Object> dataWriter) {
    this.dataWriter = dataWriter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void call(Iterator<Object> records)
      throws Exception {
    while (records.hasNext()) {
      this.dataWriter.write(records.next());
    }
  }
}
