/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.format;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;

import io.confluent.connect.cloud.storage.source.StorageObjectFormat;
import io.confluent.connect.cloud.storage.source.util.Segment;
import io.confluent.connect.cloud.storage.source.util.ValueOffset;

/**
 * Class for JsonFormat. 
 * It is used while reading JSON data.
 */
public class CloudStorageJsonFormat extends StorageObjectFormat {

  private static final Logger log = LoggerFactory.getLogger(CloudStorageJsonFormat.class);

  private static final String DEFAULT_JSON_EXTENSION = "json";
  private static final String LINE_SEPARATOR = System.lineSeparator();

  private final JsonConverter jsonConverter;
  private final String extension;

  public CloudStorageJsonFormat(CloudStorageSourceConnectorCommonConfig config) {
    jsonConverter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put("schemas.cache.size", String.valueOf(config.getSchemaCacheSize()));
    this.jsonConverter.configure(converterConfig, false);
    extension = DEFAULT_JSON_EXTENSION;
  }

  @Override
  public String getExtension() {
    return extension;
  }

  @Override
  public ValueOffset extractRecord(InputStream content, long objectSize, long offset) {
    log.trace("Attempting to extract next json record after offset {}", offset);
    Segment segment = readUntilSeparator(content, LINE_SEPARATOR, objectSize, offset);
    byte[] bytes = segment.getValue();
    SchemaAndValue schemaAndValue = jsonConverter.toConnectData("", bytes);
    return new ValueOffset(schemaAndValue, offset + segment.getLength(), segment.isLast());
  }
}
