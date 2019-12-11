/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.reader;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;

public class ParquetInputFile implements InputFile {
  private InputStream inputStream;

  ParquetInputFile(InputStream inputStream) {
    this.inputStream  = inputStream;
  }

  /*
   It return the length of inputstream
   */
  public long getLength() throws IOException {
    return this.inputStream.available();
  }

  /*
  It give the seekable input stream
   */
  public SeekableInputStream newStream() {
    return new ParquetSeekableStream(this.inputStream);
  }
}
