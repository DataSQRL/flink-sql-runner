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
package com.datasqrl.secure;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Generates a random ID string with the given number of secure random bytes. The bytes are base64
 * encoded so the string length will be longer than the number of bytes
 */
@AutoService(StandardLibraryFunction.class)
public class RandomID extends ScalarFunction implements StandardLibraryFunction {

  private static final SecureRandom random = new SecureRandom();
  private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

  public String eval(Long numBytes) {
    if (numBytes == null) {
      return null;
    }
    assert numBytes >= 0;
    byte[] buffer = new byte[numBytes.intValue()];
    random.nextBytes(buffer);
    return encoder.encodeToString(buffer);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return FlinkTypeUtil.basicNullInferenceBuilder(DataTypes.STRING(), DataTypes.BIGINT())
        .typedArguments(List.of(DataTypes.BIGINT()))
        .build();
  }
}
