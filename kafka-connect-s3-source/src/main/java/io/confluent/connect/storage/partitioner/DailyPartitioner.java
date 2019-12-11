/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.s3.source.S3SourceConnectorConfig;
import io.confluent.connect.s3.source.S3Storage;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PATH_FORMAT_CONFIG;

public class DailyPartitioner extends TimeBasedPartitioner {
  public DailyPartitioner(S3SourceConnectorConfig config, S3Storage storage) {
    super(addFormat(config), storage);
  }

  private static S3SourceConnectorConfig addFormat(S3SourceConnectorConfig config) {
    Map<String, String> values = new HashMap<>((Map<String, String>) config.values());
    String delim = config.getStorageDelimiter();
    String pathFormat = "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd";
    values.put(PATH_FORMAT_CONFIG, pathFormat);
    return new S3SourceConnectorConfig(values);
  }

}
