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
package com.datasqrl.datatype;

import com.datasqrl.function.SqrlCastFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/** Converts an annotated data type to */
public class SerializeToBytes extends ScalarFunction implements SqrlCastFunction {

  @SneakyThrows
  public byte[] eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object object) {
    DataTypeHint hint = object.getClass().getAnnotation(DataTypeHint.class);
    Class<? extends TypeSerializer> serializerClass = hint.rawSerializer();

    TypeSerializer serializer = serializerClass.newInstance();

    DataOutputSerializer dos = new DataOutputSerializer(128);

    serializer.serialize(object, dos);

    return dos.getCopyOfBuffer();
  }
}
