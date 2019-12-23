/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceTask;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.utils.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HDSourceTask extends AbstractCloudStorageSourceTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
  */
  private static final Logger log = LoggerFactory.getLogger(HDSourceTask.class);

  protected HDSourceConnectorConfig config;

  public HDSourceTask() {}

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  protected CloudStorageSourceConnectorCommonConfig createConfig(Map<String, String> props) {
    this.config = new HDSourceConnectorConfig(props);
    return this.config;
  }

  @Override
  protected CloudSourceStorage createStorage() {
    return new HDStorage(config, config.getHdfsUrl());
  }

  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }

  public void start(Map<String, String> props) {
    super.start(props);
  }

  //Visible for testing
  protected HDSourceTask(Map<String, String> settings, CloudSourceStorage storage) {
    super(settings, storage,
      new StorageObjectSourceReader(new HDSourceConnectorConfig(settings)));
  }

  // Visible for testing
  protected HDSourceTask(HDSourceConnectorConfig config,
      HDStorage storage, StorageObjectSourceReader reader) {
    folders = config.getList(HDSourceConnectorConfig.FOLDERS_CONFIG);
    this.storage = storage;
    this.config = config;
    sourceReader = reader;
    partitioner = config.getPartitioner(storage);
    maxBatchSize = config.getRecordBatchMaxSize();
  }

}