/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.format.avro;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageAvroFormat;

/**
 * Class for AvroFormat. 
 * It extends CloudStorageAvroFormat from io.confluent.connect.cloud.storage.source.format
 * 
 * <p>CloudStorageAvroFormat class can also be directly used as formatter but to support 
 * backward-compatibility, we have added AvroFormat class.</p>
 */
public class AvroFormat extends CloudStorageAvroFormat {

  public AvroFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }

}
