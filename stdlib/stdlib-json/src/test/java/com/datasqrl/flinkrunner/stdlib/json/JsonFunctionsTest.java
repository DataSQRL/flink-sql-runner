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
package com.datasqrl.flinkrunner.stdlib.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JsonFunctionsTest {
  ObjectMapper mapper = new ObjectMapper();

  @SneakyThrows
  private JsonNode readTree(String val) {
    return mapper.readTree(val);
  }

  @Nested
  class ToJsonTest {

    @Test
    void testUnicodeJson() {
      var row = Row.withNames();
      row.setField("key", "”value”");
      var rows = new Row[] {row};
      var result = JsonFunctions.TO_JSON.eval(rows);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[{\"key\":\"”value”\"}]");
    }

    @Test
    void testValidJson() {
      var json = "{\"key\":\"value\"}";
      var result = JsonFunctions.TO_JSON.eval(json);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo(json);
    }

    @Test
    void testInvalidJson() {
      var json = "Not a JSON";
      var result = JsonFunctions.TO_JSON.eval(json);
      assertThat(result).isNull();
    }

    @Test
    void testNullInput() {
      assertThat(JsonFunctions.TO_JSON.eval(null)).isNull();
    }
  }

  @Nested
  class JsonToStringTest {

    @Test
    void testNonNullJson() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_TO_STRING.eval(json);
      assertThat(result).isEqualTo("{\"key\":\"value\"}");
    }

    @Test
    void testNullJson() {
      var result = JsonFunctions.JSON_TO_STRING.eval(null);
      assertThat(result).isNull();
    }
  }

  @Nested
  class JsonObjectTest {

    @Test
    void testValidKeyValuePairs() {
      var result = JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2", "value2");
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("{\"key1\":\"value1\",\"key2\":\"value2\"}");
    }

    @Test
    void testInvalidNumberOfArguments() {
      assertThatThrownBy(() -> JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullKeyOrValue() {
      var resultWithNullValue = JsonFunctions.JSON_OBJECT.eval("key1", null);
      assertThat(resultWithNullValue).isNotNull();
      assertThat(resultWithNullValue.getJson().toString()).isEqualTo("{\"key1\":null}");
    }
  }

  @Nested
  class JsonArrayTest {

    @Test
    void testArrayWithJsonObjects() {
      var json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      var json2 = new FlinkJsonType(readTree("{\"key2\": \"value2\"}"));
      var result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]");
    }

    @Test
    void testArrayWithMixedTypes() {
      var result = JsonFunctions.JSON_ARRAY.eval("stringValue", 123, true);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[\"stringValue\",123,true]");
    }

    @Test
    void testArrayWithNullValues() {
      var result = JsonFunctions.JSON_ARRAY.eval((Object) null);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[null]");
    }
  }

  @Nested
  class JsonExtractTest {

    @Test
    void testValidPath() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key");
      assertThat(result).isEqualTo("value");
    }

    @Test
    void testValidPathBoolean() {
      var json = new FlinkJsonType(readTree("{\"key\": true}"));
      var result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key");
      assertThat(result).isEqualTo("true");
    }

    // Testing eval method with a default value for String
    @Test
    void testStringPathWithDefaultValue() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var defaultValue = "default";
      var result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertThat(result).isEqualTo(defaultValue);
    }

    // Testing eval method with a default value for boolean
    @Test
    void testBooleanPathNormalWithDefaultValue() {
      var json = new FlinkJsonType(readTree("{\"key\": true}"));
      var defaultValue = false;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key", defaultValue);
      assertThat(result).isTrue();
    }

    @Test
    void testBooleanPathWithDefaultValue() {
      var json = new FlinkJsonType(readTree("{\"key\": true}"));
      var defaultValue = false;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertThat(result).isFalse();
    }

    // Testing eval method with a default value for boolean:false
    @Test
    void testBooleanPathWithDefaultValueTrue() {
      var json = new FlinkJsonType(readTree("{\"key\": true}"));
      var defaultValue = true;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertThat(result).isTrue();
    }

    // Testing eval method with a default value for Double
    @Test
    void testDoublePathWithDefaultValue() {
      var json = new FlinkJsonType(readTree("{\"key\": 1.23}"));
      Double defaultValue = 4.56;
      var result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key", defaultValue);
      assertThat(result).isEqualTo(1.23);
    }

    // Testing eval method with a default value for Integer
    @Test
    void testIntegerPathWithDefaultValue() {
      var json = new FlinkJsonType(readTree("{\"key\": 123}"));
      Integer defaultValue = 456;
      var result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key", defaultValue);
      assertThat(result).isEqualTo(123);
    }

    @Test
    void testInvalidPath() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey");
      assertThat(result).isNull();
    }
  }

  @Nested
  class JsonQueryTest {

    @Test
    void testValidQuery() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_QUERY.eval(json, "$.key");
      assertThat(result).isEqualTo("\"value\""); // Note the JSON representation of a string value
    }

    // Test for a more complex JSON path query
    @Test
    void testComplexQuery() {
      var json = new FlinkJsonType(readTree("{\"key1\": {\"key2\": \"value\"}}"));
      var result = JsonFunctions.JSON_QUERY.eval(json, "$.key1.key2");
      assertThat(result).isEqualTo("\"value\""); // JSON representation of the result
    }

    // Test for an invalid query
    @Test
    void testInvalidQuery() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_QUERY.eval(json, "$.invalidKey");
      assertThat(result).isNull();
    }
  }

  @Nested
  class JsonExistsTest {

    @Test
    void testPathExists() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_EXISTS.eval(json, "$.key");
      assertThat(result).isTrue();
    }

    // Test for a path that exists
    @Test
    void testPathExistsComplex() {
      var json = new FlinkJsonType(readTree("{\"key1\": {\"key2\": \"value\"}}"));
      var result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.key2");
      assertThat(result).isTrue();
    }

    @Test
    void testPathDoesNotExistComplex() {
      var json = new FlinkJsonType(readTree("{\"key1\": {\"key2\": \"value\"}}"));
      var result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.nonexistentKey");
      assertThat(result).isFalse();
    }

    @Test
    void testPathDoesNotExist() {
      var json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      var result = JsonFunctions.JSON_EXISTS.eval(json, "$.nonexistentKey");
      assertThat(result).isFalse();
    }

    @Test
    void testNullInput() {
      var result = JsonFunctions.JSON_EXISTS.eval(null, "$.key");
      assertThat(result).isNull();
    }
  }

  @Nested
  class JsonConcatTest {

    @Test
    void testSimpleMerge() {
      var json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      var json2 = new FlinkJsonType(readTree("{\"key2\": \"value2\"}"));
      var result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
      assertThat(result.getJson().toString())
          .isEqualTo("{\"key1\":\"value1\",\"key2\":\"value2\"}");
    }

    @Test
    void testOverlappingKeys() {
      var json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      var json2 = new FlinkJsonType(readTree("{\"key\": \"value2\"}"));
      var result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
      assertThat(result.getJson().toString()).isEqualTo("{\"key\":\"value2\"}");
    }

    @Test
    void testNullInput() {
      var json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      var result = JsonFunctions.JSON_CONCAT.eval(json1, null);
      assertThat(result).isNull();
    }

    @Test
    void testNullInput2() {
      var json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      var result = JsonFunctions.JSON_CONCAT.eval(null, json1);
      assertThat(result).isNull();
    }
  }

  @Nested
  class JsonArrayAggAccumulatorTest {

    @Test
    void testAggregateJsonTypes() {
      var accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(
          accumulator, new FlinkJsonType(readTree("{\"key1\": \"value1\"}")));
      JsonFunctions.JSON_ARRAYAGG.accumulate(
          accumulator, new FlinkJsonType(readTree("{\"key2\": \"value2\"}")));

      var result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]");
    }

    @Test
    void testAggregateMixedTypes() {
      var accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, "stringValue");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, 123);

      var result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[\"stringValue\",123]");
    }

    @Test
    void testAccumulateNullValues() {
      var accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, (FlinkJsonType) null);
      var result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertThat(result.getJson().toString()).isEqualTo("[null]");
    }

    @Test
    void testArrayWithNullElements() {
      var json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      FlinkJsonType json2 = null; // null JSON object
      var result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertThat(result).isNotNull();
      // Depending on implementation, the result might include the null or ignore it
      assertThat(result.getJson().toString()).isEqualTo("[{\"key1\":\"value1\"},null]");
    }

    @Test
    void testRetractJsonTypes() {
      var accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      var json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      var json2 = new FlinkJsonType(readTree("{\"key\": \"value2\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json2);

      // Now retract one of the JSON objects
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, json1);

      var result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[{\"key\":\"value2\"}]");
    }

    @Test
    void testRetractNullJsonType() {
      var accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      var json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, (FlinkJsonType) null);

      // Now retract a null JSON object
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, (FlinkJsonType) null);

      var result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[{\"key\":\"value1\"}]");
    }

    @Test
    void testRetractNullFromNonExisting() {
      var accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      var json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);

      // Attempt to retract a null value that was never accumulated
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, (FlinkJsonType) null);

      var result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("[{\"key\":\"value1\"}]");
    }

    @Test
    void testMergeWithProperRetract() {
      var acc = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(acc, 1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(acc, 2);
      JsonFunctions.JSON_ARRAYAGG.accumulate(acc, 3);
      JsonFunctions.JSON_ARRAYAGG.retract(acc, 4);

      var otherAcc = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(otherAcc, 1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(otherAcc, 2);
      JsonFunctions.JSON_ARRAYAGG.accumulate(otherAcc, 3);
      JsonFunctions.JSON_ARRAYAGG.accumulate(otherAcc, 4);
      JsonFunctions.JSON_ARRAYAGG.accumulate(otherAcc, 5);
      JsonFunctions.JSON_ARRAYAGG.retract(otherAcc, 1);

      JsonFunctions.JSON_ARRAYAGG.merge(acc, List.of(otherAcc));

      var res = JsonFunctions.JSON_ARRAYAGG.getValue(acc);
      assertThat(res.getJson().toString()).isEqualTo("[1,2,3,2,3,5]");
    }
  }

  @Nested
  class JsonObjectAggAccumulatorTest {

    @Test
    void testAggregateJsonTypes() {
      var accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(
          accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(
          accumulator, "key2", new FlinkJsonType(readTree("{\"nestedKey2\": \"nestedValue2\"}")));

      var result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo(
              "{\"key1\":{\"nestedKey1\":\"nestedValue1\"},\"key2\":{\"nestedKey2\":\"nestedValue2\"}}");
    }

    @Test
    void testAggregateWithOverwritingKeys() {
      var accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value1");
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value2");

      var result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("{\"key\":\"value2\"}"); // The last value for the same key should be retained
    }

    @Test
    void testNullKey() {
      assertThatThrownBy(() -> JsonFunctions.JSON_OBJECT.eval(null, "value1"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullValue() {
      var result = JsonFunctions.JSON_OBJECT.eval("key1", null);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString()).isEqualTo("{\"key1\":null}");
    }

    @Test
    void testNullKeyValue() {
      assertThatThrownBy(() -> JsonFunctions.JSON_OBJECT.eval(null, null))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testArrayOfNullValues() {
      var result = JsonFunctions.JSON_OBJECT.eval("key1", new Object[] {null, null, null});
      assertThat(result).isNotNull();
      // The expected output might vary based on how the function is designed to handle this case
      assertThat(result.getJson().toString()).isEqualTo("{\"key1\":[null,null,null]}");
    }

    @Test
    void testRetractJsonTypes() {
      var accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(
          accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(
          accumulator, "key2", new FlinkJsonType(readTree("{\"nestedKey2\": \"nestedValue2\"}")));

      // Now retract a key-value pair
      JsonFunctions.JSON_OBJECTAGG.retract(
          accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));

      var result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("{\"key2\":{\"nestedKey2\":\"nestedValue2\"}}");
    }

    @Test
    void testRetractNullJsonValue() {
      var accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(
          accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", (FlinkJsonType) null);

      // Now retract a null value
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, "key2", (FlinkJsonType) null);

      var result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("{\"key1\":{\"nestedKey1\":\"nestedValue1\"}}");
    }

    @Test
    void testRetractNullKey() {
      var accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(
          accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, null, "someValue");

      // Attempt to retract a key-value pair where the key is null
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, null, "someValue");

      var result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertThat(result).isNotNull();
      assertThat(result.getJson().toString())
          .isEqualTo("{\"key1\":{\"nestedKey1\":\"nestedValue1\"}}");
    }
  }
}
