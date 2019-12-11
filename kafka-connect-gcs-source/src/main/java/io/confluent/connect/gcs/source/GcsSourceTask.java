/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceTask;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.gcs.source.GcsSourceConnectorConfig.STORE_URL_CONFIG;

import java.util.HashMap;
import java.util.Map;

/**
 * Task Class for GCS Source connector.
 */
public class GcsSourceTask extends AbstractCloudStorageSourceTask {

  static final Logger log = LoggerFactory.getLogger(GcsSourceTask.class);
  
  private GcsSourceConnectorConfig config;

  // Visible for testing
  public GcsSourceTask(HashMap<String, String> settings, GcsSourceStorage storage) {
    super(settings, storage,
        new StorageObjectSourceReader(new GcsSourceConnectorConfig(settings))
    );
  }

  //no-arg constructor required by Connect framework.
  public GcsSourceTask() {}

  @Override
  protected CloudStorageSourceConnectorCommonConfig createConfig(Map<String, String> props) {
    this.config = new GcsSourceConnectorConfig(props);
    return this.config;
  }

  @Override
  protected CloudSourceStorage createStorage() {
    try {
      return new GcsSourceStorage(config, config.getString(STORE_URL_CONFIG));
    } catch (Exception e) {
      throw new ConnectException("Error while creating GcsSourceStorage object {}", e) ;
    }
  }

  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }
  
}
