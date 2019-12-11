/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;


import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceTask;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;
import io.confluent.connect.utils.Version;

/**
 * Source Task class for Azure Blob Storage.
 */
public class AzureBlobStorageSourceTask extends AbstractCloudStorageSourceTask {

  static final Logger log = LoggerFactory.getLogger(AzureBlobStorageSourceTask.class);

  private AzureBlobStorageSourceConnectorConfig config;
  
  public AzureBlobStorageSourceTask() {}

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  protected CloudStorageSourceConnectorCommonConfig createConfig(Map<String, String> props) {
    this.config = new AzureBlobStorageSourceConnectorConfig(props);
    return this.config;
  }

  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }

  @Override
  protected CloudSourceStorage createStorage() {
    try {
      return new AzureBlobSourceStorage(config);
    } catch (Exception e) {
      throw new ConnectException("Error creating AzureBlobSourceStorage object", e);
    }
  }
  
  //Visible for testing
  protected AzureBlobStorageSourceTask(Map<String, String> settings, CloudSourceStorage storage) {
    super(settings, storage, 
        new StorageObjectSourceReader(new AzureBlobStorageSourceConnectorConfig(settings)));
  }
  
}