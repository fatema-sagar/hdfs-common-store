/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

import org.apache.kafka.connect.data.SchemaAndValue;

/**
 * A POJO class for holding value offset data.
 */
public class ValueOffset {

  private final SchemaAndValue record;
  private final long offset;
  private final boolean eof;

  public ValueOffset(SchemaAndValue record, long offset, boolean eof) {
    this.record = record;
    this.offset = offset;
    this.eof = eof;
  }

  public SchemaAndValue getRecord() {
    return record;
  }

  public long getOffset() {
    return offset;
  }

  public boolean isEof() {
    return eof;
  }
}
