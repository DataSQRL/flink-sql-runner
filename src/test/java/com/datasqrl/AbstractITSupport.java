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
package com.datasqrl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.datasqrl.flink.client.FlinkClient;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import feign.Feign;
import feign.Logger.ErrorLogger;
import feign.Logger.Level;
import feign.Request.Options;
import feign.Retryer;
import feign.form.FormEncoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;

@Slf4j
public class AbstractITSupport {

  protected static FlinkClient client;

  @BeforeAll
  static void waitServiceStart() {
    client = newClientBuilder().logLevel(Level.NONE).target(FlinkClient.class, serverUrl());

    await()
        .atMost(100, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              log.info("Awaiting for custody-api");
              try (var response = client.healthCheck(); ) {
                return response.status() == 200;
              }
            });
  }

  public static ObjectMapper jacksonMapper() {
    var mapper =
        new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(SerializationFeature.INDENT_OUTPUT, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .findAndRegisterModules();
    return mapper;
  }

  protected static Feign.Builder newClientBuilder() {
    var mapper = jacksonMapper();

    return Feign.builder()
        .encoder(new FormEncoder(new JacksonEncoder(mapper)))
        .decoder(new JacksonDecoder(mapper))
        .logLevel(Level.BASIC)
        .logger(new ErrorLogger())
        .options(new Options(300, SECONDS, 300, SECONDS, true))
        .retryer(Retryer.NEVER_RETRY);
  }

  protected static String serverUrl() {
    var serverPort = Optional.ofNullable(System.getProperty("server.port")).orElse("8081");
    return "http://localhost:" + serverPort;
  }
}
