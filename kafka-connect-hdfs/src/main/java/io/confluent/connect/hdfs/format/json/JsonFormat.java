package io.confluent.connect.hdfs.format.json;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.format.CloudStorageJsonFormat;

public class JsonFormat extends CloudStorageJsonFormat {
  public JsonFormat(CloudStorageSourceConnectorCommonConfig config) {
    super(config);
  }
}
