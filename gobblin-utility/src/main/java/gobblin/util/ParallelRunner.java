/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Striped;

import gobblin.configuration.State;


/**
 * A class that is responsible for running certain methods in parallel. Methods in this class returns immediately and
 * are run in a fixed-size thread pool.
 *
 * <p>
 *   This class is intended to be used in the following pattern. This example uses the serialize() method.
 *
 *   <pre> {@code
 *     Closer closer = Closer.create();
 *     try {
 *       // Do stuff
 *       ParallelRunner runner = closer.register(new ParallelRunner(threads, fs));
 *       runner.serialize(state1, outputFilePath1);
 *       // Submit more serialization tasks
 *       runner.serialize(stateN, outputFilePathN);
 *       // Do stuff
 *     } catch (Throwable e) {
 *       throw closer.rethrow(e);
 *     } finally {
 *       closer.close();
 *     }}
 *   </pre>
 *
 *   Note that calling {@link #close()} will wait for all submitted tasks to complete and then stop the
 *   {@link ParallelRunner} by shutting down the {@link ExecutorService}.
 * </p>
 *
 * @author Yinan Li
 */
public class ParallelRunner implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelRunner.class);

  public static final String PARALLEL_RUNNER_THREADS_KEY = "parallel.runner.threads";
  public static final int DEFAULT_PARALLEL_RUNNER_THREADS = 10;

  private final ExecutorService executor;
  private final FileSystem fs;

  private final List<Future<?>> futures = Lists.newArrayList();

  private final Striped<Lock> locks = Striped.lazyWeakLock(Integer.MAX_VALUE);

  public ParallelRunner(int threads, FileSystem fs) {
    this.executor = Executors.newFixedThreadPool(threads,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ParallelRunner")));
    this.fs = fs;
  }

  /**
   * Serialize a {@link State} object into a file.
   *
   * <p>
   *   This method submits a task to serialize the {@link State} object and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param state the {@link State} object to be serialized
   * @param outputFilePath the file to write the serialized {@link State} object to
   * @param <T> the {@link State} object type
   */
  public <T extends State> void serializeToFile(final T state, final Path outputFilePath) {
    // Use a Callable with a Void return type to allow exceptions to be thrown
    this.futures.add(this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        SerializationUtils.serializeState(fs, outputFilePath, state);
        return null;
      }
    }));
  }

  /**
   * Deserialize a {@link State} object from a file.
   *
   * <p>
   *   This method submits a task to deserialize the {@link State} object and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param state an empty {@link State} object to which the deserialized content will be populated
   * @param inputFilePath the input file to read from
   * @param <T> the {@link State} object type
   */
  public <T extends State> void deserializeFromFile(final T state, final Path inputFilePath) {
    this.futures.add(this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        SerializationUtils.deserializeState(fs, inputFilePath, state);
        return null;
      }
    }));
  }

  /**
   * Deserialize a list of {@link State} objects from a Hadoop {@link SequenceFile}.
   *
   * <p>
   *   This method submits a task to deserialize the {@link State} objects and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param stateClass the {@link Class} object of the {@link State} class
   * @param inputFilePath the input {@link SequenceFile} to read from
   * @param states a {@link Collection} object to store the deserialized {@link State} objects
   * @param deleteAfter a flag telling whether to delete the {@link SequenceFile} afterwards
   * @param <T> the {@link State} object type
   */
  public <T extends State> void deserializeFromSequenceFile(final Class<? extends Writable> keyClass,
      final Class<T> stateClass, final Path inputFilePath, final Collection<T> states, final boolean deleteAfter) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Closer closer = Closer.create();
        try {
          @SuppressWarnings("deprecation")
          SequenceFile.Reader reader = closer.register(new SequenceFile.Reader(fs, inputFilePath, fs.getConf()));
          Writable key = keyClass.newInstance();
          T state = stateClass.newInstance();
          while (reader.next(key, state)) {
            states.add(state);
            state = stateClass.newInstance();
          }

          if (deleteAfter) {
            HadoopUtils.deletePath(fs, inputFilePath, false);
          }
        } catch (Throwable t) {
          throw closer.rethrow(t);
        } finally {
          closer.close();
        }

        return null;
      }
    }));
  }

  /**
   * Delete a {@link Path}.
   *
   * <p>
   *   This method submits a task to delete a {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param path path to be deleted.
   */
  public void deletePath(final Path path, final boolean recursive) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Lock lock = locks.get(path.toString());
        lock.lock();
        try {
          HadoopUtils.deletePath(fs, path, recursive);
          return null;
        } finally {
          lock.unlock();
        }
      }
    }));
  }

  /**
   * Rename a {@link Path}.
   *
   * <p>
   *   This method submits a task to rename a {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @param group an optional group name for the destination path
   */
  public void renamePath(final Path src, final Path dst, final Optional<String> group) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Lock lock = locks.get(src.toString());
        lock.lock();
        try {
          if (fs.exists(src)) {
            HadoopUtils.renamePath(fs, src, dst);
            if (group.isPresent()) {
              HadoopUtils.setGroup(fs, dst, group.get());
            }
          }
          return null;
        } catch (FileAlreadyExistsException e) {
          LOGGER.warn(String.format("Failed to rename %s to %s: dst already exists", src, dst), e);
          return null;
        } finally {
          lock.unlock();
        }
      }
    }));
  }

  /**
   * Move a {@link Path}.
   *
   * <p>
   *   This method submits a task to move a {@link Path} and returns immediately
   *   after the task is submitted.
   * </p>
   *
   * @param src path to be moved
   * @param dstFs the destination {@link FileSystem}
   * @param dst the destination path
   * @param group an optional group name for the destination path
   */
  public void movePath(final Path src, final FileSystem dstFs, final Path dst, final Optional<String> group) {
    this.futures.add(this.executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Lock lock = locks.get(src.toString());
        lock.lock();
        try {
          if (fs.exists(src)) {
            HadoopUtils.movePath(fs, src, dstFs, dst);
            if (group.isPresent()) {
              HadoopUtils.setGroup(dstFs, dst, group.get());
            }
          }
          return null;
        } catch (FileAlreadyExistsException e) {
          LOGGER.warn(String.format("Failed to move %s to %s: dst already exists", src, dst), e);
          return null;
        } finally {
          lock.unlock();
        }
      }
    }));
  }

  @Override
  public void close() throws IOException {
    try {
      // Wait for all submitted tasks to complete
      for (Future<?> future : this.futures) {
        future.get();
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ExecutionException ee) {
      throw new IOException(ee.getCause());
    } finally {
      ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(LOGGER));
    }
  }
}
