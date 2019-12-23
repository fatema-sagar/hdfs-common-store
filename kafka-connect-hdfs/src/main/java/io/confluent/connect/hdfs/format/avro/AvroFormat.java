/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.format.avro;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageAvroFormat;

public class AvroFormat extends CloudStorageAvroFormat {
  public AvroFormat(CloudStorageSourceConnectorCommonConfig config) {
      super(config);
  }
}
