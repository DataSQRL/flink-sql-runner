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
package com.datasqrl.connector.postgresql.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.connector.postgresql.jdbc.SqrlPostgresOptions.OnConflictAction;
import org.junit.jupiter.api.Test;

class SqrlPostgresDialectTest {

  private static final String TABLE = "events";
  private static final String[] FIELDS = {"id", "value", "event_time"};
  private static final String[] SINGLE_KEY = {"id"};

  @Test
  void upsertWithUpdateActionUsesDoUpdateSet() {
    var dialect = new SqrlPostgresDialect(OnConflictAction.UPDATE, null);

    var sql = dialect.getUpsertStatement(TABLE, FIELDS, SINGLE_KEY).orElseThrow();

    assertThat(sql)
        .contains("ON CONFLICT (\"id\") DO UPDATE SET ")
        .contains("\"id\"=EXCLUDED.\"id\"")
        .contains("\"value\"=EXCLUDED.\"value\"")
        .contains("\"event_time\"=EXCLUDED.\"event_time\"")
        .doesNotContain("WHERE")
        .doesNotContain("DO NOTHING");
  }

  @Test
  void upsertWithIgnoreActionUsesDoNothing() {
    var dialect = new SqrlPostgresDialect(OnConflictAction.IGNORE, null);

    var sql = dialect.getUpsertStatement(TABLE, FIELDS, SINGLE_KEY).orElseThrow();

    assertThat(sql)
        .endsWith("ON CONFLICT (\"id\") DO NOTHING")
        .doesNotContain("DO UPDATE SET")
        .doesNotContain("WHERE");
  }

  @Test
  void upsertWithTimestampActionGuardsUpdateWithTimestampPredicate() {
    var dialect = new SqrlPostgresDialect(OnConflictAction.TIMESTAMP, "event_time");

    var sql = dialect.getUpsertStatement(TABLE, FIELDS, SINGLE_KEY).orElseThrow();

    assertThat(sql)
        .contains("ON CONFLICT (\"id\") DO UPDATE SET ")
        .contains("\"value\"=EXCLUDED.\"value\"")
        .endsWith(" WHERE EXCLUDED.\"event_time\" > \"events\".\"event_time\"");
  }

  @Test
  void upsertWithCompositePrimaryKeyQuotesAllKeys() {
    var dialect = new SqrlPostgresDialect(OnConflictAction.UPDATE, null);

    var sql =
        dialect.getUpsertStatement(TABLE, FIELDS, new String[] {"id", "tenant"}).orElseThrow();

    assertThat(sql).contains("ON CONFLICT (\"id\", \"tenant\") DO UPDATE SET ");
  }

  @Test
  void upsertWithTimestampActionPicksGivenTimestampColumn() {
    var dialect = new SqrlPostgresDialect(OnConflictAction.TIMESTAMP, "value");

    var sql = dialect.getUpsertStatement(TABLE, FIELDS, SINGLE_KEY).orElseThrow();

    assertThat(sql).endsWith(" WHERE EXCLUDED.\"value\" > \"events\".\"value\"");
  }
}
