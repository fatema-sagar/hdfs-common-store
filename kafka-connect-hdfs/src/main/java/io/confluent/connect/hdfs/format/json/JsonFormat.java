/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.format.json;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageJsonFormat;

/**
 * Class for JsonFormat.
 * It extends CloudStorageJsonFormat from io.confluent.connect.cloud.storage.source.format
 *
 * <p>CloudStorageJsonFormat class can also be directly used as formatter but to support
 * backward-compatibility, we have added JsonFormat class.</p>
 */
public class JsonFormat extends CloudStorageJsonFormat {
  public JsonFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }
}
