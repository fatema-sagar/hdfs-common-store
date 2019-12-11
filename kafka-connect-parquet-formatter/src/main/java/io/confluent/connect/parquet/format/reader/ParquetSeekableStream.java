/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.parquet.io.DelegatingSeekableInputStream;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class extends the DelegatingSeekableInputStream i.e
 * used to skip the bytes in the inputstream
 */
public class ParquetSeekableStream
        extends DelegatingSeekableInputStream {
  private InputStream inputStream;
  private long position;

  ParquetSeekableStream(InputStream inputStream) {
    super(inputStream);
    this.inputStream = inputStream;
    this.position = 0;
  }

  @Override
  public long getPos() throws IOException {
    return this.position;
  }

  /**
   *This method is used to go to the particular byte
   */
  @Override
  public void seek(long l) throws IOException {
    if (inputStream instanceof FSDataInputStream) {
      ((FSDataInputStream) inputStream).seek(l);
    } else if (inputStream instanceof FileInputStream) {
      if (l < 0) {
        throw new EOFException(
                FSExceptionMessages.NEGATIVE_SEEK);
      }
      ((FileInputStream) inputStream)
              .getChannel().position(l);
    }
    this.position = l;
  }
}
