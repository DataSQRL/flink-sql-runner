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
package com.datasqrl.vector;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class FlinkVectorTypeSerializerSnapshot implements TypeSerializerSnapshot<FlinkVectorType> {

  private Class<FlinkVectorTypeSerializer> serializerClass;

  public FlinkVectorTypeSerializerSnapshot() {
    this.serializerClass = FlinkVectorTypeSerializer.class;
  }

  @Override
  public int getCurrentVersion() {
    return 1;
  }

  @Override
  public void writeSnapshot(DataOutputView out) throws IOException {
    out.writeUTF(FlinkVectorTypeSerializer.class.getName());
  }

  @Override
  public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
      throws IOException {
    String className = in.readUTF();
    try {
      this.serializerClass =
          (Class<FlinkVectorTypeSerializer>) Class.forName(className, true, userCodeClassLoader);
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to find serializer class: " + className, e);
    }
  }

  @Override
  public TypeSerializer restoreSerializer() {
    try {
      return serializerClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(
          "Failed to instantiate serializer class: " + serializerClass.getName(), e);
    }
  }

  @Override
  public TypeSerializerSchemaCompatibility resolveSchemaCompatibility(
      TypeSerializer newSerializer) {
    if (newSerializer.getClass() == this.serializerClass) {
      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    } else {
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}
