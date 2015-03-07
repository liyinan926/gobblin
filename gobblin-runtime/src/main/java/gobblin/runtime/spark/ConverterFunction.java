package gobblin.runtime.spark;

import org.apache.spark.api.java.function.FlatMapFunction;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;


/**
 * A Spark {@link org.apache.spark.api.java.function.FlatMapFunction} that wraps the functionality of
 * a Gobblin {@link gobblin.converter.Converter}.
 *
 * @author ynli
 */
public class ConverterFunction implements FlatMapFunction<Object, Object> {

  private final Converter converter;
  private final Object schema;
  private final WorkUnitState workUnitState;

  public ConverterFunction(Converter converter, Object schema, WorkUnitState workUnitState) {
    this.converter = converter;
    this.schema = schema;
    this.workUnitState = workUnitState;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<Object> call(Object input)
      throws Exception {
    return this.converter.convertRecord(this.schema, input, this.workUnitState);
  }
}
