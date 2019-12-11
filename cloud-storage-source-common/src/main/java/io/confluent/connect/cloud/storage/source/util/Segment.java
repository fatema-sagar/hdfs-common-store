/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

public class Segment {
  private final byte[] value;
  private final int length; // The bytes length + line separator length
  private final boolean last;

  public Segment(byte[] value, int length, boolean last) {
    this.value = value;
    this.length = length;
    this.last = last;
  }

  public byte[] getValue() {
    return value;
  }

  public int getLength() {
    return length;
  }

  public boolean isLast() {
    return last;
  }
}
