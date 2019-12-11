/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage.format.bytearray;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageByteArrayFormat;

/**
 * Class for ByteArrayFormat.
 * It extends CloudStorageByteArrayFormat from io.confluent.connect.cloud.storage.source.format
 * 
 */
public class ByteArrayFormat extends CloudStorageByteArrayFormat {

  public ByteArrayFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }

}
