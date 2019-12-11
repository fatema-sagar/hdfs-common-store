/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.format;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.StorageObjectFormat;
import io.confluent.connect.cloud.storage.source.util.Segment;
import io.confluent.connect.cloud.storage.source.util.ValueOffset;

/**
 * Class for ByteArrayFormat. 
 * It is used while reading byte array data.
 */
public class CloudStorageByteArrayFormat extends StorageObjectFormat {

  private static final Logger log = LoggerFactory.getLogger(CloudStorageByteArrayFormat.class);

  private final ByteArrayConverter converter;
  private final String extension;
  private final String lineSeparator;

  public CloudStorageByteArrayFormat(CloudStorageSourceConnectorCommonConfig config) {
    converter = new ByteArrayConverter();
    extension = config.getByteArrayExtension();
    lineSeparator = config.getFormatByteArrayLineSeparator();
  }

  @Override
  public String getExtension() {
    return extension;
  }

  @Override
  public ValueOffset extractRecord(InputStream content, long objectSize, long offset) {
    log.trace("Attempting to extract next byte array record after offset {}", offset);
    Segment segment = readUntilSeparator(content, lineSeparator, objectSize, offset);
    byte[] bytes = segment.getValue();
    SchemaAndValue schemaAndValue = converter.toConnectData("", bytes);
    return new ValueOffset(schemaAndValue, offset + segment.getLength(), segment.isLast());
  }

}
