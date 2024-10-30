package com.datasqrl;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;

class JsonEnvVarDeserializer extends JsonDeserializer<String> {

  private Map<String, String> env;

  public JsonEnvVarDeserializer() {
    env = System.getenv();
  }

  public JsonEnvVarDeserializer(Map<String, String> env) {
    this.env = env;
  }

  @Override
  public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    String value = p.getText();
    return replaceWithEnv(this.env, value);
  }

  public String replaceWithEnv(Map<String, String> env, String value) {
    Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
    Matcher matcher = pattern.matcher(value);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      String key = matcher.group(1);
      String envVarValue = env.get(key);
      if (envVarValue != null) {
        matcher.appendReplacement(result, Matcher.quoteReplacement(envVarValue));
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }
}