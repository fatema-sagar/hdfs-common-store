/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage.format.json;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageJsonFormat;

/**
 * Class for JsonFormat. 
 * It extends CloudStorageJsonFormat from io.confluent.connect.cloud.storage.source.format
 * 
 */
public class JsonFormat extends CloudStorageJsonFormat {

  public JsonFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }

}
