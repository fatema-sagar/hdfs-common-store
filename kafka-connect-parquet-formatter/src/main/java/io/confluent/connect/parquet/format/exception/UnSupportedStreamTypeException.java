/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.exception;

import java.io.IOException;

public class UnSupportedStreamTypeException extends IOException {
  public UnSupportedStreamTypeException(String cause) {
    super(cause);
  }
}
