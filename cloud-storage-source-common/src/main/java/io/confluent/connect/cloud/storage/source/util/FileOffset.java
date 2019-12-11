/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

import java.util.Objects;

/**
 * A POJO to store file offset.
 */
public class FileOffset {
  
  private String filename;
  private long offset;
  private boolean eof;

  public FileOffset(String filename, long offset, boolean eof) {
    this.filename = filename;
    this.offset = offset;
    this.eof = eof;
  }

  public FileOffset(FileOffset other) {
    this.filename = other.filename;
    this.offset = other.offset;
    this.eof = other.eof;
  }

  public static FileOffset emptyOffset() {
    return new FileOffset(null, 0L, true);
  }

  public String filename() {
    return filename;
  }

  public long offset() {
    return offset;
  }

  public boolean eof() {
    return eof;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FileOffset that = (FileOffset) o;
    return offset == that.offset
        && eof == that.eof
        && Objects.equals(filename, that.filename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filename, offset, eof);
  }

  @Override
  public String toString() {
    return "FileOffset{"
        + "filename='" + filename + '\''
        + ", offset=" + offset
        + ", eof=" + eof
        + '}';
  }

}
