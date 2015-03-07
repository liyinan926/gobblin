package gobblin.runtime.spark;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil;
import org.apache.spark.rdd.HadoopPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.FileBasedJobLock;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLock;
import gobblin.runtime.JobState;
import gobblin.runtime.mapreduce.GobblinInputFormat;
import gobblin.runtime.mapreduce.GobblinInputSplit;
import gobblin.runtime.mapreduce.GobblinRecordReader;
import gobblin.source.workunit.WorkUnit;


/**
 * A type of {@link gobblin.runtime.JobLauncher}s that launches Gobblin jobs on Spark.
 *
 * @author ynli
 */
public class SparkJobLauncher extends AbstractJobLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkJobLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final JavaSparkContext sparkContext;
  private final Configuration conf;
  private final FileSystem fs;

  public SparkJobLauncher(Properties properties) throws Exception {
    super(properties);
    this.sparkContext = new JavaSparkContext();
    this.conf = new Configuration();
    URI fsUri = URI.create(this.properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.fs = FileSystem.get(fsUri, this.conf);
  }

  @Override
  protected void runJob(String jobName, Properties jobProps, JobState jobState, List<WorkUnit> workUnits)
      throws Exception {
    Path sparkJobDir = new Path(jobProps.getProperty(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), jobName);
    if (this.fs.exists(sparkJobDir)) {
      LOGGER.warn("Job working directory already exists for job " + jobName);
      this.fs.delete(sparkJobDir, true);
    }

    // Add framework jars to the classpath for the mappers/reducer
    if (jobProps.containsKey(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY)) {
      addJars(jobProps.getProperty(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY));
    }
    // Add job-specific jars to the classpath for the mappers
    if (jobProps.containsKey(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      addJars(jobProps.getProperty(ConfigurationKeys.JOB_JAR_FILES_KEY));
    }

    // Add other files (if any) the job depends on to DistributedCache
    if (jobProps.containsKey(ConfigurationKeys.JOB_LOCAL_FILES_KEY)) {
      addFiles(jobProps.getProperty(ConfigurationKeys.JOB_LOCAL_FILES_KEY));
    }

    // Add files (if any) already on HDFS that the job depends on to DistributedCache
    if (jobProps.containsKey(ConfigurationKeys.JOB_HDFS_FILES_KEY)) {
      addFiles(jobProps.getProperty(ConfigurationKeys.JOB_HDFS_FILES_KEY));
    }

    JavaNewHadoopRDD<?, ?> hadoopRDD = (JavaNewHadoopRDD<?, ?>) this.sparkContext
        .newAPIHadoopRDD(this.conf, GobblinInputFormat.class, NullWritable.class, getRecordClass(jobProps));

    Map<Integer, GobblinInputSplit> indexToWorkUnitMapping = Maps.newHashMap();
    Map<Integer, Object> indexToSchemaMapping = Maps.newHashMap();

    GobblinInputFormat<?, ?> gobblinInputFormat = new GobblinInputFormat();
    SparkHadoopMapReduceUtil sparkHadoopMapReduceUtil = new SparkHadoopMapReduceUtil();

    for (Partition partition : hadoopRDD.partitions()) {
      HadoopPartition hadoopPartition = (HadoopPartition) partition;
      GobblinInputSplit inputSplit = (GobblinInputSplit) hadoopPartition.inputSplit().value();

      indexToWorkUnitMapping.put(hadoopPartition.index(), inputSplit);

      TaskAttemptID taskAttemptID = sparkHadoopMapReduceUtil.newTaskAttemptID("", 0, true, hadoopPartition.index(), 0);
      indexToSchemaMapping.put(hadoopPartition.index(), ((GobblinRecordReader<?, ?>) gobblinInputFormat
          .createRecordReader(inputSplit, sparkHadoopMapReduceUtil.newTaskAttemptContext(this.conf, taskAttemptID)))
          .getSchema());
    }

    this.sparkContext.addFile(serializeInputSplits(indexToWorkUnitMapping, sparkJobDir));
    this.sparkContext.addFile(serializeSchema(indexToSchemaMapping, sparkJobDir));
  }

  @Override
  protected JobLock getJobLock(String jobName, Properties jobProps)
      throws IOException {
    return new FileBasedJobLock(this.fs, jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY), jobName);
  }

  @Override
  public void cancelJob(Properties jobProps)
      throws JobException {
    this.sparkContext.close();
  }

  /**
   * Add framework or job-specific jars the job depends on to the classpath. The jar files can be on
   * local file system, HDFS, or any Hadoop-supported file systems.
   *
   * @param jarFileList comma-separated list of jar files
   * @throws IOException
   */
  private void addJars(String jarFileList) throws IOException {
    for (String jarFile : SPLITTER.split(jarFileList)) {
      this.sparkContext.addJar(jarFile);
    }
  }

  /**
   * Add files the job depends on. The files can be on local file system, HDFS, or any Hadoop-supported file systems.
   *
   * @param jobFileList comma-separated list of files
   * @throws IOException
   */
  private void addFiles(String jobFileList) throws IOException {
    for (String jobFile : SPLITTER.split(jobFileList)) {
      this.sparkContext.addFile(jobFile);
    }
  }

  private Class<?> getRecordClass(Properties jobProps) throws ClassNotFoundException {
    return Class.forName(jobProps.getProperty(""));
  }

  @SuppressWarnings("all")
  private String serializeInputSplits(Map<Integer, GobblinInputSplit> indexToInputSplitMapping, Path sparkWorkDir)
      throws IOException {
    Path indexToInputSplitMappingFile = new Path(sparkWorkDir, "index-to-input-split-mapping");
    Closer closer = Closer.create();
    try {
      SequenceFile.Writer writer = closer.register(SequenceFile
          .createWriter(this.fs, this.conf, indexToInputSplitMappingFile, IntWritable.class, GobblinInputSplit.class));
      for (Map.Entry<Integer, GobblinInputSplit> entry : indexToInputSplitMapping.entrySet()) {
        writer.append(new IntWritable(entry.getKey()), entry.getValue());
      }

      return indexToInputSplitMappingFile.toString();
    } catch (Throwable t) {
      LOGGER.error("Failed to serialize input splits to file " + indexToInputSplitMappingFile);
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @SuppressWarnings("all")
  private String serializeSchema(Map<Integer, Object> indexToSchemaMapping, Path sparkWorkDir)
      throws IOException {
    Path indexToSchemaMappingFile = new Path(sparkWorkDir, "index-to-input-split-mapping");
    Closer closer = Closer.create();
    try {
      SequenceFile.Writer writer = closer.register(SequenceFile
          .createWriter(this.fs, this.conf, indexToSchemaMappingFile, IntWritable.class, ObjectWritable.class));
      for (Map.Entry<Integer, Object> entry : indexToSchemaMapping.entrySet()) {
        writer.append(new IntWritable(entry.getKey()), new ObjectWritable(entry.getValue()));
      }

      return indexToSchemaMappingFile.toString();
    } catch (Throwable t) {
      LOGGER.error("Failed to serialize input splits to file " + indexToSchemaMappingFile);
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
