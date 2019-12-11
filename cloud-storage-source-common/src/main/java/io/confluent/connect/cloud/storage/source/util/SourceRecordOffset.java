/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A POJO class to hold source record and its offset.
 */
public class SourceRecordOffset {
  private final SourceRecord record;
  private final long offset;
  private final boolean eof;

  public SourceRecordOffset(SourceRecord record, long offset, boolean eof) {
    this.record = record;
    this.offset = offset;
    this.eof = eof;
  }

  public SourceRecord getRecord() {
    return record;
  }

  public long getOffset() {
    return offset;
  }

  public boolean isEof() {
    return eof;
  }
}
