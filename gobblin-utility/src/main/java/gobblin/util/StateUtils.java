package gobblin.util;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import gobblin.configuration.State;


/**
 * A utility class to work with the Gobblin {@link gobblin.configuration.State} class.
 *
 * @author ynli
 */
public class StateUtils {

  /**
   * Get a {@link gobblin.configuration.State} object from a Hadoop {@link org.apache.hadoop.conf.Configuration} object.
   *
   * @param conf a Hadoop {@link org.apache.hadoop.conf.Configuration} object
   * @return a {@link gobblin.configuration.State} object
   */
  public static State fromConfiguration(Configuration conf) {
    State state = new State();
    for (Map.Entry<String, String> entry: conf) {
      state.setProp(entry.getKey(), entry.getValue());
    }

    return state;
  }

  /**
   * Get a Hadoop {@link org.apache.hadoop.conf.Configuration} object from a {@link gobblin.configuration.State} object.
   *
   * @param state a {@link gobblin.configuration.State} object
   * @return a Hadoop {@link org.apache.hadoop.conf.Configuration} object
   */
  public static Configuration toConfiguration(State state) {
    Configuration conf = new Configuration();
    for (String key : state.getPropertyNames()) {
      conf.set(key, state.getProp(key));
    }

    return conf;
  }
}
