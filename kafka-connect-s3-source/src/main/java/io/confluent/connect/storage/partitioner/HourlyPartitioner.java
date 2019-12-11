/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.Map;

import io.confluent.connect.s3.source.S3SourceConnectorConfig;
import io.confluent.connect.s3.source.S3Storage;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PATH_FORMAT_CONFIG;

public class HourlyPartitioner extends TimeBasedPartitioner {
  public HourlyPartitioner(S3SourceConnectorConfig config, S3Storage storage) {
    super(addFormat(config), storage);
  }

  @SuppressWarnings("unchecked")
  private static S3SourceConnectorConfig addFormat(S3SourceConnectorConfig config) {
    Map<String, String> values = (Map<String, String>) config.values();
    String delim = config.getStorageDelimiter();
    String pathFormat =
        "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd" + delim + "'hour'=HH";
    values.put(PATH_FORMAT_CONFIG, pathFormat);
    return new S3SourceConnectorConfig(values);
  }

}
