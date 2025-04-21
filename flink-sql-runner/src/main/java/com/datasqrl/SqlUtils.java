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

import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;

/** Utility class for parsing SQL scripts. */
@UtilityClass
class SqlUtils {

  private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
  private static final String LINE_DELIMITER = "\n";

  private static final String COMMENT_PATTERN = "(--.*)|(((/\\*)+?[\\w\\W]+?(\\*/)+))";

  private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
  private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
  private static final String ESCAPED_BEGIN_CERTIFICATE = "======BEGIN CERTIFICATE=====";
  private static final String ESCAPED_END_CERTIFICATE = "=====END CERTIFICATE=====";

  /**
   * Parses SQL statements from a script.
   *
   * @param script The SQL script content.
   * @return A list of individual SQL statements.
   */
  public static List<String> parseStatements(String script) {
    var formatted =
        formatSqlFile(script)
            .replaceAll(BEGIN_CERTIFICATE, ESCAPED_BEGIN_CERTIFICATE)
            .replaceAll(END_CERTIFICATE, ESCAPED_END_CERTIFICATE)
            .replaceAll(COMMENT_PATTERN, "")
            .replaceAll(ESCAPED_BEGIN_CERTIFICATE, BEGIN_CERTIFICATE)
            .replaceAll(ESCAPED_END_CERTIFICATE, END_CERTIFICATE);

    List<String> statements = new ArrayList<>();

    StringBuilder current = null;
    var statementSet = false;
    for (String line : formatted.split("\n")) {
      var trimmed = line.trim();
      if (trimmed.isBlank()) {
        continue;
      }
      if (current == null) {
        current = new StringBuilder();
      }
      if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
        statementSet = true;
      }
      current.append(trimmed);
      current.append("\n");
      if (trimmed.endsWith(STATEMENT_DELIMITER)) {
        if (!statementSet || trimmed.equalsIgnoreCase("END;")) {
          statements.add(current.toString());
          current = null;
          statementSet = false;
        }
      }
    }
    return statements;
  }

  /**
   * Formats the SQL file content to ensure proper statement termination.
   *
   * @param content The SQL file content.
   * @return Formatted SQL content.
   */
  public static String formatSqlFile(String content) {
    var trimmed = content.trim();
    var formatted = new StringBuilder();
    formatted.append(trimmed);
    if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
      formatted.append(STATEMENT_DELIMITER);
    }
    formatted.append(LINE_DELIMITER);
    return formatted.toString();
  }
}
