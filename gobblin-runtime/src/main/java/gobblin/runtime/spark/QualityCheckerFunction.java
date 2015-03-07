package gobblin.runtime.spark;

import org.apache.spark.api.java.function.Function;

import gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;


/**
 * A Spark {@link org.apache.spark.api.java.function.Function} that wraps the functionality of
 * a Gobblin quality checker.
 *
 * @author ynli
 */
public class QualityCheckerFunction implements Function<Object, Boolean> {

  private final RowLevelPolicyChecker rowLevelPolicyChecker;
  private final RowLevelPolicyCheckResults results;

  public QualityCheckerFunction(RowLevelPolicyChecker rowLevelPolicyChecker, RowLevelPolicyCheckResults results) {
    this.rowLevelPolicyChecker = rowLevelPolicyChecker;
    this.results = results;
  }

  @Override
  public Boolean call(Object input)
      throws Exception {
    return this.rowLevelPolicyChecker.executePolicies(input, this.results);
  }
}
