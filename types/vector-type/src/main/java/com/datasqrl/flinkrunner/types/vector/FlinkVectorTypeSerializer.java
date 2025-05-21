/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.flinkrunner.types.vector;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class FlinkVectorTypeSerializer extends TypeSerializer<FlinkVectorType> {

  private static final long serialVersionUID = -8481213829625869500L;

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public FlinkVectorType createInstance() {
    return new FlinkVectorType(null);
  }

  @Override
  public FlinkVectorType copy(FlinkVectorType from) {
    return new FlinkVectorType(from.getValue());
  }

  @Override
  public FlinkVectorType copy(FlinkVectorType from, FlinkVectorType reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1; // indicates that this serializer does not have a fixed length
  }

  @Override
  public void serialize(FlinkVectorType record, DataOutputView target) throws IOException {
    target.writeInt(record.getValue().length); // First write the length of the array
    for (double v : record.getValue()) {
      target.writeDouble(v); // Write each double value
    }
  }

  @Override
  public FlinkVectorType deserialize(DataInputView source) throws IOException {
    var length = source.readInt();
    var array = new double[length];
    for (var i = 0; i < length; i++) {
      array[i] = source.readDouble();
    }
    return new FlinkVectorType(array);
  }

  @Override
  public FlinkVectorType deserialize(FlinkVectorType reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeUTF(source.readUTF());
  }

  @Override
  public TypeSerializer<FlinkVectorType> duplicate() {
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FlinkVectorTypeSerializer;
  }

  @Override
  public int hashCode() {
    return FlinkVectorTypeSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<FlinkVectorType> snapshotConfiguration() {
    return new FlinkVectorTypeSerializerSnapshot();
  }
}
