/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner.types.json;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FlinkJsonTypeSerializer extends TypeSerializer<FlinkJsonType> {

  ObjectMapper mapper = new ObjectMapper();

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public FlinkJsonType createInstance() {
    return new FlinkJsonType(null);
  }

  @Override
  public FlinkJsonType copy(FlinkJsonType from) {
    return new FlinkJsonType(from.getJson());
  }

  @Override
  public FlinkJsonType copy(FlinkJsonType from, FlinkJsonType reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1; // indicates that this serializer does not have a fixed length
  }

  @Override
  public void serialize(FlinkJsonType record, DataOutputView target) throws IOException {
    var jsonData = mapper.writeValueAsBytes(record.getJson());
    target.writeInt(jsonData.length);
    target.write(jsonData);
  }

  @Override
  public FlinkJsonType deserialize(DataInputView source) throws IOException {
    var length = source.readInt();
    var jsonData = new byte[length];
    source.readFully(jsonData);
    return new FlinkJsonType(mapper.readTree(jsonData));
  }

  @Override
  public FlinkJsonType deserialize(FlinkJsonType reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    var length = source.readInt();
    var jsonData = new byte[length];
    source.readFully(jsonData);
    target.writeInt(length);
    target.write(jsonData);
  }

  @Override
  public TypeSerializer<FlinkJsonType> duplicate() {
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FlinkJsonTypeSerializer;
  }

  @Override
  public int hashCode() {
    return FlinkJsonTypeSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<FlinkJsonType> snapshotConfiguration() {
    return new FlinkJsonTypeSerializerSnapshot();
  }
}
