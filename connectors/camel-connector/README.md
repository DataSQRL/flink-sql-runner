# Camel Connector

A Flink SQL source connector, `'connector' = 'camel'`, for long-tail systems read through
[Apache Camel](https://camel.apache.org/): SaaS APIs, queues, SFTP drops and published files. A
source is one Java class written against `camel-source-sdk`. It holds the endpoint and the
per-record logic, and Camel does the I/O. The table's `source.class` option names the class.

The contract the class implements is the mode. Modes differ only in where the read position lives:

| Mode | Contract | Use when | Position kept in Flink state | Typical sources |
| :--- | :--- | :--- | :--- | :--- |
| Cursor | `CursorSource<OUT, POS, IN>` | The system can return changes since a position | Cursor + current poll timestamp | SaaS REST APIs, databases polled on `updated_at` |
| Ack | `AckSource<OUT>` | The system tracks delivery and redelivers unacked messages | None (pending acks are held in memory only) | SQS, JMS / IBM MQ, AMQP, mail |
| File | `FileSource<OUT, IN>` | Data arrives as files or published documents | Seen-set of file versions + current poll timestamp | SFTP/FTP, network shares, published CSVs |

Use cursor mode whenever the system can rewind, even if it also supports acks. Every source reads
with parallelism 1 on a single fetcher thread that owns the CamelContext. Delivery is
at-least-once, and tables are append-only: duplicates are resolved downstream by key.

## Getting it onto the UDF path

The connector is not part of the runner's uber jar or Docker image. A job that reads a Camel source
puts the following in the directory it passes as `--udfpath`:

- `camel-connector` and its dependencies (`camel-source-sdk`, `camel-core-engine`, `camel-file`, ...)
- the Camel component of every system the sources talk to, e.g. `camel-http`, `camel-salesforce`,
  `camel-jms`
- the jar with the source classes, and any library they use

Flink and SLF4J are provided by the runner; leave them out. The runner loads every jar of the
directory with `ADD JAR`, so the factory, the Camel components and the source classes are found
through the job's classloader.

Every Camel jar must be the Camel version the connector is built against (`camel.version` in the
root pom), because Camel needs its components at its core's exact version. Import
`org.apache.camel:camel-bom` at that version in the build that assembles the directory. The
connector checks the classpath when the job is planned:

- Planning fails when a Camel jar has another version, naming each such jar.
- Planning fails when a component is missing: one a `camel.component.<name>.*` option configures,
  or the one an ack source's endpoint or a file source's location names. A component the source
  binds with `@Bind` under that name counts as present.
- A cursor source's requests are only built at runtime, so a request to a missing component fails
  the job there, without retries. So does an endpoint whose component comes from a property
  placeholder.

[`testing/camel-source-sample/pom.xml`](../../testing/camel-source-sample/pom.xml) shows the Maven
setup: the sources depend on `camel-source-sdk` (compile) and `camel-connector` (runtime), and the
`package` phase copies the runtime dependencies and the module's jar into `target/camel-udfs`.

## Writing a source

A source class is public, has a public no-argument constructor, implements exactly one of the three
contracts, and emits a Java record. `@CamelSource` marks it for discovery. All contract methods run
on the fetcher thread. `map` returning `null` drops the record; `map` throwing skips the record,
logs it at WARN and counts it in the `numRecordsFailed` metric. Dropped and failed records still
count as consumed.

The examples below are in [`testing/camel-source-sample`](../../testing/camel-source-sample), each
with its table under `src/main/resources/sample/camel`.

### Cursor mode

`CursorSource<OUT, POS, IN>`: the starting position, the request for everything since a position,
optionally the next page of a response and how to split it into records, the per-record map, and
the position of an emitted record.

```java
@CamelSource
public class Accounts implements CursorSource<Accounts.Account, Instant, JsonNode> {

  public record Account(String id, String name, String industry,
      BigDecimal annualRevenue, Instant modifiedAt, boolean deleted) {}

  public Instant initial() { return Instant.EPOCH; }

  public Request request(Instant since) {
    return Request.to("salesforce:queryAll?rawPayload=true").body(
        "SELECT Id, Name, Industry, AnnualRevenue, SystemModstamp, IsDeleted"
            + " FROM Account"
            + " WHERE SystemModstamp >= " + Soql.datetime(since)  // whole seconds
            + " ORDER BY SystemModstamp, Id");
  }

  public Optional<Request> next(Exchange response) {              // follow pages, no LIMIT
    JsonNode body = Json.body(response);
    if (body.path("done").asBoolean(true)) return Optional.empty();
    return Optional.of(Request.to("salesforce:queryMore?rawPayload=true")
        .body(body.get("nextRecordsUrl").asText()));
  }

  public Iterable<JsonNode> records(Exchange response) { return Json.body(response).get("records"); }

  public Account map(JsonNode r) {
    return new Account(
        r.get("Id").asText(), r.get("Name").asText(),
        r.path("Industry").asText(null),
        Types.decimal(r.get("AnnualRevenue"), 18, 2),
        Types.instant(r.get("SystemModstamp")),
        r.get("IsDeleted").asBoolean());
  }

  public Instant position(Account a) { return a.modifiedAt().truncatedTo(ChronoUnit.SECONDS); }
}
```

```sql
CREATE TABLE accounts (
  id STRING,
  name STRING,
  industry STRING,
  annualRevenue DECIMAL(18, 2),
  modifiedAt TIMESTAMP_LTZ(3),
  deleted BOOLEAN,
  ingested_at TIMESTAMP_LTZ(3) METADATA,
  WATERMARK FOR ingested_at AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'camel',
  'source.class' = 'sample.camel.Accounts',
  'poll.interval' = '5 min',
  'camel.component.salesforce.clientId' = '{{env:SF_CLIENT_ID}}',
  'camel.component.salesforce.clientSecret' = '{{env:SF_CLIENT_SECRET}}'
);
```

`Json.body(exchange)` parses a response once and keeps the tree on the exchange, so `records` and
`next` can both read a streamed body.

### Ack mode

`AckSource<OUT>`: the consumer endpoint, and the per-record map of each received exchange.

```java
@CamelSource
public class Shipments implements AckSource<Shipments.Shipment> {

  public record Shipment(String shipmentId, String status, String carrier, Instant updatedAt) {}

  @Bind("cf")
  public ConnectionFactory cf() {
    return new ActiveMQConnectionFactory(System.getenv("BROKER_URL"));
  }

  public String endpoint() { return "jms:queue:shipments?connectionFactory=#cf"; }

  public Shipment map(Exchange e) {
    JsonNode j = Json.body(e);
    if ("TEST".equals(j.path("env").asText())) return null;
    return new Shipment(
        j.get("id").asText(),
        j.get("status").asText().toUpperCase(),
        j.path("carrier").asText(null),
        Types.instant(j.get("ts")));
  }
}
```

```sql
CREATE TABLE shipments (
  shipmentId STRING,
  status STRING,
  carrier STRING,
  updatedAt TIMESTAMP_LTZ(3),
  ingested_at TIMESTAMP_LTZ(3) METADATA,
  WATERMARK FOR ingested_at AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'camel',
  'source.class' = 'sample.camel.Shipments'
);
```

### File mode

`FileSource<OUT, IN>`: the location to list, how to parse one file into raw records, and the
per-record map.

```java
@CamelSource
public class GlPostings implements FileSource<GlPostings.Posting, String> {

  public record Posting(String postingId, String account, BigDecimal amount, LocalDate postDate) {}

  public String location() {
    return "sftp://erp.example.com/outbound/gl"
        + "?username={{env:ERP_USER}}&privateKeyFile={{env:ERP_KEY}}"
        + "&include=GL_.*\\.dat";
  }

  public Iterable<String> records(Exchange file) {
    return FixedWidth.lines(file, "IBM037");                     // EBCDIC
  }

  public Posting map(String line) {
    if (line.startsWith("HDR") || line.startsWith("TRL")) return null;
    return new Posting(
        line.substring(0, 12).trim(),
        line.substring(12, 22).trim(),
        Types.decimal(line.substring(22, 37).trim(), 18, 2),
        Types.date(line.substring(37, 45), "yyyyMMdd"));
  }
}
```

```sql
CREATE TABLE gl_postings (
  postingId STRING,
  account STRING,
  amount DECIMAL(18, 2),
  postDate DATE,
  ingested_at TIMESTAMP_LTZ(3) METADATA,
  WATERMARK FOR ingested_at AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'camel',
  'source.class' = 'sample.camel.GlPostings',
  'file.ready' = 'trigger:.done'
);
```

`Csv.parse` and `FixedWidth.lines` read a file lazily; the source closes it once the file is read.

### Beans, placeholders and component options

- **`@Bind("name")`** on a public no-argument method of the source binds its result in Camel's
  registry under `name` before the endpoint is created, for beans Camel can't configure from
  properties, such as a JMS connection factory. The endpoint refers to it as `#name`. It works in all
  three modes.
- **`{{env:NAME}}`** in an endpoint, request, location or `camel.component.*` value is a Camel
  property placeholder, resolved from the TaskManager's environment. Keep credentials out of the
  source class and the table options this way: the runner's own `${NAME}` substitution happens on
  the client, so its values end up in the job graph and the client log.
- **`camel.component.<component>.<option>`** table options configure a Camel component before it
  starts, e.g. `'camel.component.salesforce.clientId' = '...'`. An option the component doesn't have
  fails the job with its name.

The helpers in `camel-source-sdk`:

| Helper | For |
| :--- | :--- |
| `Json` | `parse(text)`, and `body(exchange)` parsed once per exchange |
| `Types` | `decimal(value, precision, scale)` rounded half-up, `instant` (ISO-8601 with `Z`, `+00:00` or `+0000`), `date(value, pattern)`, `truncate(value, maxLength)` for logs |
| `Csv` | `parse(file, format)` with commons-csv; `Csv.NO_HEADER` is RFC 4180 without a header row |
| `FixedWidth` | `lines(file, charset)`, splitting on `\r\n`, `\n`, `\r` and EBCDIC's NEL |
| `Soql` | `datetime(instant)`, a SOQL dateTime literal in whole seconds |

Source classes log through the SLF4J API, which Flink provides.

## Table options

| Key | Type | Default | Modes | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `source.class` | String | required | all | The source class |
| `fetch.timeout` | Duration | 1 s | all | How long one Camel receive waits, and the longest the fetcher blocks while idle |
| `retry.max-attempts` | Integer | 10 | all | Failed fetches in a row before the job fails; 0 turns retries off |
| `retry.initial-backoff` | Duration | 1 min | all | Delay before the first retry, doubling after each failure |
| `retry.max-backoff` | Duration | 30 min | all | Upper bound of the delay between retries |
| `scan.bounded` | Boolean | false | all | Bounded read, see below |
| `poll.interval` | Duration | 5 min | cursor, file | Time between polls once a poll is complete |
| `poll.cron` | Quartz cron expression | none | cursor, file | When polls start, e.g. `0 0 6 * * ?` for 06:00 daily, in `table.local-time-zone`; set it or `poll.interval`, not both |
| `rate-limit` | Integer, per minute | none | cursor, file | Max Camel requests or listings per minute |
| `cursor.start` | String | `initial()` | cursor | The position to start from, parsed by the position type, see below |
| `cursor.reset` | `never`, `per-poll` | `never` | cursor | `per-poll` pages from `initial()` on every poll: a full refresh |
| `cursor.lookback` | Duration | 0 | cursor | Starts every request this far before the position |
| `ack.max-pending` | Integer | 10000 | ack | Max messages held awaiting a checkpoint |
| `ack.empty-polls` | Integer | 3 | ack | With `scan.bounded`, empty receives in a row before the source finishes |
| `file.ready` | `stable-size`, `trigger:<suffix>`, `none` | `stable-size` | file | When a listed file is ready to read |
| `file.order` | `name`, `mtime`, `none` | `name` | file | The order files are read in |
| `file.snapshot-unit` | `file`, `listing` | `file` | file | What one snapshot is |
| `file.seen-retention` | Duration | 30 d | file | Seen-set eviction horizon |
| `file.min-row-ratio` | Double | none | file | Fail if a unit shrinks below this ratio of the previous one |
| `camel.component.<name>.<option>` | String | none | all | A Camel component option |

An option of another mode, such as `file.order` on a cursor source, fails planning with the option
and the mode. A table with a primary key fails planning too.

**Bounded reads.** With `scan.bounded`, a cursor source finishes after one complete poll, a file
source after one listing is fully processed, and an ack source after `ack.empty-polls` empty
receives in a row. Under `execution.runtime-mode = BATCH`, cursor and file sources are bounded
without the option. Ack sources are rejected in BATCH mode, because without checkpoints there is no
safe point to acknowledge: run them in streaming mode with `scan.bounded`, and the final checkpoint
fires the acks.

## Metadata columns

| Key | Type | Modes | Value |
| :--- | :--- | :--- | :--- |
| `ingested_at` | `TIMESTAMP_LTZ(3) NOT NULL` | all | The timestamp of the record's unit (a poll, a file or a listing), shared by its records |
| `file` | `STRING NOT NULL` | file | The file's path relative to the location, or the URL |
| `row` | `BIGINT NOT NULL` | file | The 1-based ordinal of the record within its file, counting dropped and failed records |
| `exchange_id` | `STRING NOT NULL` | ack | Camel's id for the exchange |
| `headers` | `MAP<STRING NOT NULL, STRING> NOT NULL` | ack | The message headers, converted to strings by Camel |

Declare them with `METADATA` or `METADATA FROM '<key>'`, e.g.
`file_name STRING METADATA FROM 'file'`. A key of another mode fails planning.

With `WATERMARK FOR ingested_at AS SOURCE_WATERMARK()`, the source emits a watermark at a unit's
timestamp once the unit completes with new rows, then marks itself idle until the next unit.

## Types

The record type must be a Java record, and the table must declare exactly its components, by name
and in the record's order; nested `ROW` columns follow the same rule. A column the record lacks, a
component the table lacks, a column out of order, or a column type that can't hold its component
fails planning with a message naming the column.

| Java type | Column type |
| :--- | :--- |
| `String` | `STRING` |
| `boolean` / `Boolean` | `BOOLEAN` |
| `int` / `Integer` | `INT` |
| `long` / `Long` | `BIGINT` |
| `double` / `Double` | `DOUBLE` |
| `BigDecimal` | `DECIMAL(p, s)` |
| `LocalDate` | `DATE` |
| `LocalDateTime` | `TIMESTAMP(3)` |
| `Instant` | `TIMESTAMP_LTZ(3)` |
| `List<T>` | `ARRAY<T>` |
| `Map<K, V>` | `MAP<K, V>` |
| a nested record | `ROW<...>` with the nested record's components |

Other Java types Flink can convert to the column's type work too. A `null` component yields SQL
`NULL` in its column.

## Behaviour per mode

### Ack mode

- One split, assigned to subtask 0. A single fetcher thread calls `ConsumerTemplate.receive` and
  runs `map`. The CamelContext is created lazily on that thread and lives as long as the source
  reader.
- Every exchange, including dropped and failed ones, is held until a checkpoint that covers it
  completes. Then `ConsumerTemplate.doneUoW` runs on a separate acker thread, which fires the
  component's own commit (delete the file, delete the SQS message, ...). Nothing is restored from
  state; the source system redelivers whatever was never acknowledged.
- That only holds for a component that commits when its exchange completes. One that commits
  inside its own `receive` (`sjms`, for example) loses whatever no checkpoint covered when the job
  fails, and nothing checks for it. So far `file`, `jms`, `amqp` and `activemq6` have a failover
  test.
- `jms` and the components built on it (`amqp`, `activemq`, `activemq6`) also commit inside
  `receive`, so the source reads their queue through a transacted JMS session it owns instead. A
  transaction commits everything the session received, so the commit waits until a completed
  checkpoint covers every received message, and receiving pauses from a checkpoint's snapshot
  until that commit. On a quiet queue that is about as long as the checkpoint takes; under a
  steady flow the commit takes one more checkpoint, so the source receives in roughly every other
  checkpoint interval. The endpoint supplies the connection factory, queue, selector and message
  mapping; camel-jms's other consumer options don't apply, and topics are rejected.
- `ack.max-pending` pauses receiving while that many exchanges wait for a checkpoint. The
  checkpoint interval must stay below any visibility or lease timeout of the source system.
- Camel takes at most one message from the source system ahead of a receive: the source sets the
  endpoint's `pollingConsumerQueueSize` to 1, overriding the URI. A batch the component's own poll
  has already fetched waits on top of that, such as up to `maxMessagesPerPoll` SQS messages, so
  keep such batches small next to `ack.max-pending`.
- Every record carries the timestamp of the poll that received it.
- With `retry.max-attempts` above 0, Camel's fetch errors back off exponentially and fail the job
  after that many failures in a row. With 0, Camel logs a failed poll and polls again.

### Cursor mode

- Requests go through Camel's `ProducerTemplate`. A poll follows `next(response)` page by page; if
  the last page moved the position, it requests again straight away, otherwise the poll is complete
  and the next one starts after `poll.interval`, or at the next `poll.cron` match.
- The emitter moves the checkpointed position to each emitted record's position, so a restore
  re-requests from there. Records must arrive in position order: within one response and its
  pages, a record behind the previous one (for a `Comparable` position) fails the job.
- `cursor.lookback` starts every request that far before the position, so rows that concurrent
  writers commit out of position order are still read. Rows in the window are emitted again and
  deduplicated downstream; they never move the position back. The position must be `Comparable`,
  and `CursorSource.rewind` subtracts the window: the default handles time types such as
  `Instant`, and a composite cursor overrides it. Doesn't combine with `cursor.reset = per-poll`.
- `cursor.reset = per-poll` pages through everything from `initial()` on every poll, as a snapshot.
- `cursor.start` replaces `initial()`: a fresh start and every `per-poll` refresh begin there, while
  a restore continues from the checkpointed position. The text is parsed by the position type's
  public static `parse` or `valueOf` method, which `Instant`, `LocalDate`, `LocalDateTime`, `Long`
  and `Integer` have; a `String` position is taken as is, and a composite position needs such a
  method of its own. A value it can't parse fails planning.
- The position is written to state with its Flink serializer, together with the serializer's
  snapshot, so a compatible change to the position type survives a savepoint.
- With `retry.max-attempts = 0`, a failed request fails the job straight away.
- A request blocks the fetcher until Camel returns, including any retries the component makes on
  its own, and a cancel waits only 30 seconds for the fetcher. Turn those retries off and rely on
  the `retry.*` options instead: HttpClient, for one, sleeps through a 429's `Retry-After` inside the
  request. The `RecentChanges` sample does this with an `httpClientConfigurer`.

### File mode

- The location is a `file:`, `ftp:`, `ftps:` or `sftp:` endpoint, or one `http(s):` URL. It is
  never modified: the source forces `noop=true` and rejects `delete`, `move` and `preMove`.
- A file's version is its relative path, mtime and size. Camel's consumer lists the location with the
  seen-set plugged in as its idempotent repository, so seen versions are skipped while listing.
  A version enters the checkpointed seen-set only once its last record is emitted, and is forgotten
  after `file.seen-retention` without appearing in a listing.
- A URL is fetched with a conditional GET (`If-None-Match`, `If-Modified-Since`); its version is a
  SHA-256 of the content, so a republished but unchanged document isn't read again.
- `file.ready`: `stable-size` (Camel's `readLock=changed`), `trigger:<suffix>` (Camel's
  `doneFileName`) or `none`. `file.order`: `name`, `mtime` or `none`.
- `file.snapshot-unit`: with `file` each new file is a unit with its own timestamp. With
  `listing` a poll first lists all versions, and if the set changed, every file of the listing is
  read at one timestamp, for multi-part snapshots.
- A restore re-reads a half-emitted unit from the start with the same timestamp, before anything
  else. Downstream deduplicates by key, or by `file` and `row` for keyless data.
- `file.min-row-ratio` fails the job when a unit has fewer than that ratio of the previous unit's
  rows, so a truncated download can't look like mass deletion.

### Shared by cursor and file mode

Every record of one unit (a cursor poll, or a file or listing) carries the unit's timestamp T,
`max(now, lastT + 1ms)`, with both kept in split state. A unit without rows emits no watermark, so
an unchanged poll never closes a snapshot. `rate-limit` caps Camel calls (requests and listings)
per minute.

With `poll.cron`, a completed poll waits for the next time the expression matches instead of
`poll.interval`. The first poll after a start or restore runs right away, as with an interval.
