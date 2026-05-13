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
package com.datasqrl.flinkrunner;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auto.service.AutoService;
import io.github.classgraph.AnnotationClassRef;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.junit.jupiter.api.Test;

class FactoryAutoServiceTest {

  private static final Set<String> FIRST_PARTY_ARTIFACTS =
      Set.of(
          "datagen-connectors",
          "flexible-csv-format",
          "flexible-json-format",
          "flink-sql-runner",
          "kafka-safe-connector",
          "postgresql-connector",
          "stdlib-iceberg");

  @Test
  void dynamicTableFactoriesAreRegisteredWithAutoService() {
    try (var scanResult =
        new ClassGraph()
            .enableAnnotationInfo()
            .enableClassInfo()
            .acceptPackages("com.datasqrl", "org.apache.flink.streaming.connectors.kafka")
            .scan()) {

      var factories =
          Stream.concat(
                  scanResult.getClassesImplementing(DynamicTableSourceFactory.class).stream(),
                  scanResult.getClassesImplementing(DynamicTableSinkFactory.class).stream())
              .filter(FactoryAutoServiceTest::isFirstPartyClass)
              .distinct()
              .toList();

      assertThat(factories).isNotEmpty();

      var factoriesMissingAutoService =
          factories.stream()
              .filter(factory -> !hasAutoServiceFactoryAnnotation(factory))
              .map(ClassInfo::getName)
              .toList();

      assertThat(factoriesMissingAutoService)
          .as("Dynamic table factories missing @AutoService(Factory.class)")
          .isEmpty();
    }
  }

  private static boolean isFirstPartyClass(ClassInfo classInfo) {
    var classpathElement = classInfo.getClasspathElementURI().toString();
    return FIRST_PARTY_ARTIFACTS.stream().anyMatch(classpathElement::contains);
  }

  private static boolean hasAutoServiceFactoryAnnotation(ClassInfo classInfo) {
    var annotationInfo = classInfo.getAnnotationInfo(AutoService.class.getName());
    if (annotationInfo == null) {
      return false;
    }

    var serviceTypes = annotationInfo.getParameterValues().getValue("value");
    if (serviceTypes instanceof Object[] array) {
      return Stream.of(array).anyMatch(FactoryAutoServiceTest::isFactoryClassRef);
    }

    return isFactoryClassRef(serviceTypes);
  }

  private static boolean isFactoryClassRef(Object serviceType) {
    return serviceType instanceof AnnotationClassRef classRef
        && Factory.class.getName().equals(classRef.getName());
  }
}
