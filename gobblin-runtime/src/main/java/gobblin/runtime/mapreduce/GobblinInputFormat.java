package gobblin.runtime.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskState;
import gobblin.source.Source;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.StateUtils;


/**
 * A custom {@link org.apache.hadoop.mapreduce.InputFormat} that wraps a Gobblin {@link gobblin.source.Source}.
 *
 * @author ynli
 */
public class GobblinInputFormat<K, V> extends InputFormat<K, V> {

  @SuppressWarnings("unchecked")
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    List<InputSplit> inputSplits = Lists.newArrayList();

    Configuration conf = jobContext.getConfiguration();

    JobState jobState = new JobState(conf.get(ConfigurationKeys.JOB_NAME_KEY), conf.get(ConfigurationKeys.JOB_ID_KEY));
    jobState.addAll(StateUtils.fromConfiguration(conf));

    StateStore taskStateStore = new FsStateStore(
        jobState.getProp(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI),
        jobState.getProp(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), TaskState.class);
    List<WorkUnitState> previousWorkUnitStates;
    if (taskStateStore.exists(jobState.getJobName(), "current" + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX)) {
      // Read the task states of the most recent run of the job
      previousWorkUnitStates = (List<WorkUnitState>) taskStateStore
          .getAll(jobState.getJobName(), "current" + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX);
    } else {
      previousWorkUnitStates = Lists.newArrayList();
    }

    String sourceClass = jobState.getProp(ConfigurationKeys.SOURCE_CLASS_KEY);
    try {
      SourceState sourceState = new SourceState(jobState, previousWorkUnitStates);
      Source<?, ?> source = (Source<?, ?>) Class.forName(sourceClass).newInstance();
      for (WorkUnit workUnit : source.getWorkunits(sourceState)) {
        inputSplits.add(new GobblinInputSplit(workUnit));
      }

      return inputSplits;
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (InstantiationException ie) {
      throw new IOException(ie);
    } catch (IllegalAccessException iae) {
      throw new IOException(iae);
    }
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    RecordReader<K, V> recordReader = new GobblinRecordReader<K, V>();
    recordReader.initialize(inputSplit, taskAttemptContext);
    return recordReader;
  }
}
