/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage.format.avro;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageAvroFormat;

/**
 * Class for AvroFormat. 
 * It extends CloudStorageAvroFormat from io.confluent.connect.cloud.storage.source.format
 * 
 */
public class AvroFormat extends CloudStorageAvroFormat {

  public AvroFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }

}
