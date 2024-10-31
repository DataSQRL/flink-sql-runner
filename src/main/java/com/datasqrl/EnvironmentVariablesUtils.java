package com.datasqrl;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

@UtilityClass
public class EnvironmentVariablesUtils {

  private static final Pattern ENVIRONMENT_VARIABLE_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

  public static String replaceWithEnv(String command, Map<String, String> envVariables) {
    String substitutedStr = command;
    StringBuffer result = new StringBuffer();
    // First pass to replace environment variables
    Matcher matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(substitutedStr);
    while (matcher.find()) {
      String key = matcher.group(1);
      String envValue = envVariables.getOrDefault(key, "");
      matcher.appendReplacement(result, Matcher.quoteReplacement(envValue));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  public void validateEnvironmentVariables(TreeMap<String, String> envVariables, String script) {
    Matcher matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(script);

    Set<String> scriptEnvironmentVariables = new TreeSet<>();
    while (matcher.find()) {
      scriptEnvironmentVariables.add(matcher.group(1));
    }

    scriptEnvironmentVariables.removeAll(envVariables.keySet());

    if (!scriptEnvironmentVariables.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Could not find the following environment variables: %s",
              scriptEnvironmentVariables));
    }
  }
}
