/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.format.bytearray;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageByteArrayFormat;

/**
 * Class for ByteArrayFormat.
 * It extends CloudStorageByteArrayFormat from io.confluent.connect.cloud.storage.source.format
 * 
 * <P>CloudStorageByteArrayFormat class can also be directly used as formatter but to support 
 * backward-compatibility, we have added ByteArrayFormat class.</p>
 */
public class ByteArrayFormat extends CloudStorageByteArrayFormat {

  public ByteArrayFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }

}
