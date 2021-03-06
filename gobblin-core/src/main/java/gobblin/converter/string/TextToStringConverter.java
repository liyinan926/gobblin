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

package gobblin.converter.string;

import org.apache.hadoop.io.Text;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;


/**
 * An implementation of {@link Converter} that converts input records of type {@link Text} to strings.
 *
 * @author Yinan Li
 */
@SuppressWarnings("unused")
public class TextToStringConverter extends Converter<Object, Object, Text, String> {

  @Override
  public Converter<Object, Object, Text, String> init(WorkUnitState workUnit) {
    super.init(workUnit);
    return this;
  }

  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<String> convertRecord(Object outputSchema, Text inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<String>(inputRecord.toString());
  }
}
