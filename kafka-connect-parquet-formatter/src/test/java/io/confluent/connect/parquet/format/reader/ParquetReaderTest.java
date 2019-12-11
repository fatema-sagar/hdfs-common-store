/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.parquet.format.reader;


import io.confluent.connect.parquet.format.exception.UnSupportedStreamTypeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ParquetReaderTest {

  private static final String TEMP_FILE_PATH = "src/test/testResources/test.parquet";
  private static final int NO_OF_RECORDS = 200000;

  @Before
  public void createTempFile() throws IOException {
    ParquetUtilTest.parquetRecordWriter(NO_OF_RECORDS, TEMP_FILE_PATH);
  }

  @After
  public void deleteTempFile() throws IOException {
    File file = new File(TEMP_FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void fileInputStreamTest() throws IOException {
    InputStream inputStream = new FileInputStream(new File(TEMP_FILE_PATH));
    RecordExtractor recordExtractor = new RecordExtractor();
    List<ParquetSchemaRecord> list = recordExtractor.getParquetRecords(inputStream);
    Assert.assertEquals(NO_OF_RECORDS, list.size());
  }

  @Test
  public void fSDataInputStreamTest() throws IOException {
    FileSystem fileSystem = FileSystem.newInstance(new Configuration());
    InputStream inputStream = new FSDataInputStream(fileSystem.open(new Path(TEMP_FILE_PATH)));
    RecordExtractor recordExtractor = new RecordExtractor();
    List<ParquetSchemaRecord> list = recordExtractor.getParquetRecords(inputStream);
    Assert.assertEquals(NO_OF_RECORDS, list.size());
  }

  @Test(expected = UnSupportedStreamTypeException.class)
  public void unSupportedFileSystem() throws IOException {
    InputStream inputStream = new InputStream() {
      @Override
      public int read () throws IOException {
        return 0;
      }
    };
    RecordExtractor recordExtractor = new RecordExtractor();
    recordExtractor.getParquetRecords(inputStream);
  }
}
