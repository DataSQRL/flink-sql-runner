/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.flinkrunner.connector.camel.source.cursor;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Writes user-typed state values, such as a cursor position, together with their serializer's
 * snapshot. A restore reads the value with the serializer that wrote it, so a savepoint survives a
 * compatible change to the type, such as a field added to a POJO, and an incompatible change fails
 * with a clear message instead of misreading bytes.
 */
final class TypedValues {

  private TypedValues() {}

  static <T> void write(TypeSerializer<T> serializer, T value, DataOutputView out)
      throws IOException {
    TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration());
    serializer.serialize(value, out);
  }

  static <T> T read(TypeSerializer<T> serializer, DataInputView in) throws IOException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = TypedValues.class.getClassLoader();
    }
    TypeSerializerSnapshot<T> written =
        TypeSerializerSnapshot.readVersionedSnapshot(in, classLoader);
    TypeSerializerSchemaCompatibility<T> compatibility =
        serializer.snapshotConfiguration().resolveSchemaCompatibility(written);
    if (compatibility.isIncompatible()) {
      throw new IOException(
          "The position type changed incompatibly since the checkpoint was taken: "
              + written.restoreSerializer()
              + " cannot be read as "
              + serializer);
    }
    return written.restoreSerializer().deserialize(in);
  }
}
