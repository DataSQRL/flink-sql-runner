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
package com.datasqrl.flinkrunner.connector.camel.source.file;

import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.connector.camel.source.RecordContext;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs file mode on the single fetcher thread: lists the location, reads every file version not
 * seen before, and hands its mapped records to the emitter a batch at a time.
 *
 * <p>With snapshot unit {@code file}, each new file is its own unit with its own timestamp T, and
 * its version is seen once its last record is emitted. With snapshot unit {@code listing}, a poll
 * first lists all versions without reading them; if the set differs from the last unit's, every
 * file of the listing is read at one T.
 *
 * <p>After a restore, a unit that was half emitted at the checkpoint is re-read from the start with
 * the same T, before anything else. The fetcher runs ahead of the emitter, so it keeps its own copy
 * of the seen-set, which also holds every version received but not yet fully emitted.
 */
final class FileSplitReader<OUT, IN, R> implements SplitReader<FileRecord<R>, FileSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitReader.class);

  private static final int MAX_BATCH = 500;
  private static final int MAX_LOGGED_RECORD = 500;

  private enum Phase {
    /** Waiting for the next poll. */
    IDLE,
    /** Snapshot unit listing: listing every version without reading any. */
    PROBING,
    /** Looking for the next file to read. */
    LISTING,
    /** Reading a file's records. */
    READING
  }

  private final FileSettings<OUT, IN, R> settings;
  private final FileSource<OUT, IN> source;
  private final FileLister lister;
  private final boolean listingUnits;
  private final Counter droppedRecords;
  private final Counter failedRecords;

  private String splitId;
  private PollClock clock;

  /** Versions seen or being read, with the last time a listing contained them. */
  private final Map<String, Long> known = new HashMap<>();

  private long lastUnitRows = -1;
  private long resumeT = PollClock.NONE;
  private String resumeKey;

  private Phase phase = Phase.IDLE;
  private long nextPollAt;

  /** The unit in progress, or {@link PollClock#NONE}. */
  private long unitT = PollClock.NONE;

  private long unitRows;

  /** Snapshot unit listing: the versions of the unit, and those already read. */
  private Set<String> unitKeys = Set.of();

  private final Set<String> unitRead = new HashSet<>();

  private FileLister.ListedFile file;
  private Iterable<IN> fileRecords;
  private Iterator<IN> fileIterator;
  private long fileRows;

  FileSplitReader(
      FileSettings<OUT, IN, R> settings,
      FileLister lister,
      Counter droppedRecords,
      Counter failedRecords) {
    this.settings = settings;
    this.source = settings.source();
    this.lister = lister;
    this.listingUnits = settings.snapshotUnit() == CamelFileSource.SnapshotUnit.LISTING;
    this.droppedRecords = droppedRecords;
    this.failedRecords = failedRecords;
  }

  @Override
  public RecordsWithSplitIds<FileRecord<R>> fetch() throws IOException {
    if (splitId == null) {
      return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
    }
    List<FileRecord<R>> records = new ArrayList<>();
    boolean finished =
        switch (phase) {
          case IDLE -> startPoll();
          case PROBING -> probe(records);
          case LISTING -> listNext(records);
          case READING -> read(records);
        };
    String id = splitId;
    if (finished) {
      LOG.info("Completed the single listing of a bounded file source, finishing");
      splitId = null;
    }
    return new RecordsBySplits<>(
        records.isEmpty() ? Map.of() : Map.of(id, records), finished ? Set.of(id) : Set.of());
  }

  private boolean startPoll() {
    long wait = nextPollAt - System.currentTimeMillis();
    if (wait > 0) {
      sleepQuietly(Math.min(wait, settings.poll().fetchTimeout().toMillis()));
      return false;
    }
    lister.startListing();
    phase = listingUnits ? Phase.PROBING : Phase.LISTING;
    return false;
  }

  /** Snapshot unit listing: decides whether this poll reads the listing. */
  private boolean probe(List<FileRecord<R>> records) throws IOException {
    FileLister.ListedFile next = lister.next(key -> true);
    if (next == null) {
      return false;
    }
    if (next != FileLister.DONE) {
      lister.release(next);
    }
    Set<String> listing = lister.listed();
    if (unitT == PollClock.NONE && resumeT == PollClock.NONE) {
      if (listing.isEmpty() || listing.equals(known.keySet())) {
        if (listing.isEmpty() && !known.isEmpty()) {
          LOG.warn("The listing of {} is empty, keeping the last snapshot", settings.location());
        }
        return pollDone(settings.poll().pollInterval().toMillis());
      }
      unitT = clock.next();
      unitRows = 0;
      unitRead.clear();
      records.add(FileRecord.unitStart(unitT, null));
    } else if (unitT == PollClock.NONE) {
      unitT = resumeT;
      resumeT = PollClock.NONE;
      unitRows = 0;
      unitRead.clear();
      records.add(FileRecord.unitStart(unitT, null));
    }
    // A unit still open from an earlier poll keeps its T and what it already read.
    unitKeys = listing;
    lister.startListing();
    phase = Phase.LISTING;
    return false;
  }

  private boolean listNext(List<FileRecord<R>> records) throws IOException {
    FileLister.ListedFile next = lister.next(this::skip);
    if (next == null) {
      return false;
    }
    if (next == FileLister.DONE) {
      return listingDone(records);
    }
    if (listingUnits) {
      unitRead.add(next.key());
    } else {
      known.put(next.key(), System.currentTimeMillis());
      if (resumeKey != null) {
        unitT = resumeT;
        resumeKey = null;
        resumeT = PollClock.NONE;
      } else {
        unitT = clock.next();
      }
      unitRows = 0;
      records.add(FileRecord.unitStart(unitT, next.key()));
    }
    openFile(next);
    return read(records);
  }

  private boolean skip(String key) {
    if (listingUnits) {
      return !unitKeys.contains(key) || unitRead.contains(key);
    }
    // After a restore, the half-emitted file comes first.
    return resumeKey != null ? !resumeKey.equals(key) : known.containsKey(key);
  }

  private boolean listingDone(List<FileRecord<R>> records) throws IOException {
    long now = System.currentTimeMillis();
    if (listingUnits) {
      if (!unitRead.containsAll(unitKeys)) {
        Set<String> missing = new HashSet<>(unitKeys);
        missing.removeAll(unitRead);
        LOG.warn(
            "{} of {} files of the listing at {} aren't ready or are gone ({}), "
                + "continuing the unit on the next poll",
            missing.size(),
            unitKeys.size(),
            unitT,
            missing);
        return pollDone(retryDelayMs());
      }
      checkRowRatio(unitRows);
      records.add(FileRecord.unitEnd(unitT, Set.copyOf(unitKeys), unitRows, now));
      known.clear();
      unitKeys.forEach(key -> known.put(key, now));
      lastUnitRows = unitRows;
      unitT = PollClock.NONE;
      return pollDone(settings.poll().pollInterval().toMillis());
    }

    if (resumeKey != null) {
      LOG.warn(
          "{} was half read at {} but is no longer listed. Its rows at that timestamp "
              + "stay incomplete.",
          resumeKey,
          resumeT);
      records.add(FileRecord.unitAbandoned(resumeT));
      resumeKey = null;
      resumeT = PollClock.NONE;
      // Only the half-read file was considered; list everything else right away.
      lister.startListing();
      return false;
    }
    Set<String> listed = lister.listed();
    records.add(FileRecord.listed(listed, now));
    for (String key : listed) {
      known.computeIfPresent(key, (k, lastListed) -> now);
    }
    known.values().removeIf(lastListed -> lastListed < now - settings.seenRetention().toMillis());
    return pollDone(settings.poll().pollInterval().toMillis());
  }

  /** Ends the poll. Returns whether the split is finished. */
  private boolean pollDone(long nextPollInMs) {
    phase = Phase.IDLE;
    boolean unitOpen = unitT != PollClock.NONE;
    if (settings.poll().bounded() && !unitOpen) {
      return true;
    }
    nextPollAt = System.currentTimeMillis() + nextPollInMs;
    return false;
  }

  private long retryDelayMs() {
    // A bounded run ignores the poll interval, but must not spin on a file that isn't ready.
    return settings.poll().bounded()
        ? settings.poll().fetchTimeout().toMillis()
        : settings.poll().pollInterval().toMillis();
  }

  private void openFile(FileLister.ListedFile next) throws IOException {
    file = next;
    fileRows = 0;
    try {
      fileRecords = source.records(next.exchange());
      fileIterator = fileRecords.iterator();
    } catch (Exception e) {
      throw readFailed(e);
    }
    phase = Phase.READING;
  }

  private boolean read(List<FileRecord<R>> records) throws IOException {
    try {
      while (records.size() < MAX_BATCH && fileIterator.hasNext()) {
        IN raw = fileIterator.next();
        fileRows++;
        R value = convert(raw);
        if (value != null) {
          records.add(FileRecord.record(value, unitT));
        }
      }
      if (fileIterator.hasNext()) {
        return false;
      }
    } catch (RuntimeException e) {
      throw readFailed(e);
    }
    closeFile();
    unitRows += fileRows;
    phase = Phase.LISTING;
    if (!listingUnits) {
      checkRowRatio(unitRows);
      records.add(
          FileRecord.unitEnd(unitT, Set.of(file.key()), unitRows, System.currentTimeMillis()));
      lastUnitRows = unitRows;
      unitT = PollClock.NONE;
    }
    file = null;
    return false;
  }

  private R convert(IN raw) {
    try {
      OUT value = source.map(raw);
      if (value == null) {
        droppedRecords.inc();
        return null;
      }
      return settings
          .converter()
          .convert(value, new RecordContext(unitT, file.exchange(), fileRows));
    } catch (Exception e) {
      failedRecords.inc();
      LOG.warn(
          "Skipping row {} of {} that failed to map or convert. Record: {}",
          fileRows,
          file.name(),
          truncated(raw),
          e);
      return null;
    }
  }

  private void checkRowRatio(long rows) throws IOException {
    double ratio = settings.minRowRatio();
    if (ratio > 0 && lastUnitRows > 0 && rows < ratio * lastUnitRows) {
      throw new IOException(
          "The unit at "
              + unitT
              + " of "
              + settings.location()
              + " has "
              + rows
              + " rows, fewer than "
              + ratio
              + " of the previous unit's "
              + lastUnitRows
              + ". Refusing to treat it as a complete snapshot.");
    }
  }

  private IOException readFailed(Exception cause) {
    return new IOException("Failed to read " + file.name() + " from " + settings.location(), cause);
  }

  private void closeFile() {
    closeQuietly(fileIterator);
    closeQuietly(fileRecords);
    lister.release(file);
    fileIterator = null;
    fileRecords = null;
  }

  private static void closeQuietly(Object resource) {
    if (resource instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.debug("Failed to close {}", resource, e);
      }
    }
  }

  private static String truncated(Object raw) {
    String text = String.valueOf(raw);
    return text.length() <= MAX_LOGGED_RECORD ? text : text.substring(0, MAX_LOGGED_RECORD) + "...";
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<FileSplit> splitsChanges) {
    if (!(splitsChanges instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          "Unsupported splits change " + splitsChanges.getClass());
    }
    for (FileSplit split : splitsChanges.splits()) {
      splitId = split.splitId();
      known.clear();
      known.putAll(split.seen());
      lastUnitRows = split.lastUnitRows();
      clock = new PollClock(split.lastT());
      resumeT = split.unitT();
      resumeKey = listingUnits ? null : split.unitKey();
      clock.observe(resumeT);
      if (!listingUnits && resumeT != PollClock.NONE && resumeKey == null) {
        // A unit started without a file can't be resumed; nothing of it was emitted.
        resumeT = PollClock.NONE;
      }
      LOG.info(
          "Reading {} in file mode, {} versions seen{}",
          settings.location(),
          known.size(),
          resumeT != PollClock.NONE ? ", resuming the unit at " + resumeT : "");
    }
  }

  @Override
  public void wakeUp() {
    // fetch() blocks for at most the fetch timeout, plus one Camel call.
  }

  @Override
  public void close() {
    if (file != null) {
      closeQuietly(fileIterator);
      closeQuietly(fileRecords);
    }
    // The CamelContext belongs to the reader, which outlives this fetcher.
  }
}
