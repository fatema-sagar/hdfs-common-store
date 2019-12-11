/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.format;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.StorageObjectFormat;
import io.confluent.connect.cloud.storage.source.util.ValueOffset;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

/**
 * Class for StringFormat.
 * It is used while reading String data.
 */
public class CloudStorageStringFormat extends StorageObjectFormat {

  private static final Logger log = LoggerFactory.getLogger(CloudStorageStringFormat.class);
  private static final String EXTENSION = ".txt";
  private int readBufferSize = 128 * 1024;
  List<String> dataLine;
  ListIterator<String> iterator = null;

  public CloudStorageStringFormat(CloudStorageSourceConnectorCommonConfig config) {
    readBufferSize =  config.getSchemaCacheSize();
    dataLine = new ArrayList<String>();
  }

  public ValueOffset extractRecord(InputStream inputStream, long objectSize, long offset) {
    String record;
    try {
      if (offset == 0) {
        dataLine = getDataFromInputStream(inputStream);
        iterator = dataLine.listIterator();
      }
      log.trace("Attempting to extract next string record after offset {}", offset);
      record = iterator.next();
    } catch (IOException e) {
      throw new ConnectException("Could not process next String record: ", e);
    }
    return new ValueOffset(new SchemaAndValue(null, record),
            offset + 1, !iterator.hasNext());
  }

  /**
   * convert the inputstream into list of data
   * @param inputStream Input stream of file data
   * @return list of string data
   */
  private List<String> getDataFromInputStream(InputStream inputStream) throws IOException {
    String line;
    List<String> stringDataLine = new ArrayList<>();
    BufferedReader bufferedReaderTemp = new BufferedReader(
            new InputStreamReader(inputStream, "UTF-8"), readBufferSize);
    line = bufferedReaderTemp.readLine();
    if (line == null) {
      return Collections.emptyList();
    }
    stringDataLine.add(line);
    while (true) {
      line = bufferedReaderTemp.readLine();
      if (line == null) {
        break;
      }
      stringDataLine.add(line);
    }
    return stringDataLine;
  }

  public String getExtension() {
    return EXTENSION;
  }
}

