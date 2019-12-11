/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.util.Map;
import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceTask;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.cloud.storage.source.util.StorageObjectSourceReader;
import io.confluent.connect.utils.Version;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.STORE_URL_CONFIG;

/**
 * Source task class for S3.
 */
public class S3SourceTask extends AbstractCloudStorageSourceTask {
  
  private static final Logger log = LoggerFactory.getLogger(S3SourceTask.class);

  private S3SourceConnectorConfig config;

  public S3SourceTask() {}

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  protected CloudStorageSourceConnectorCommonConfig createConfig(Map<String, String> props) {
    this.config = new S3SourceConnectorConfig(props);
    return this.config;
  }

  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }

  @Override
  protected CloudSourceStorage createStorage() {
    return new S3Storage(config, config.getString(STORE_URL_CONFIG));
  }
  
  // Visible for testing
  protected S3SourceTask(Map<String, String> settings, CloudSourceStorage storage) {
    super(settings, storage,
        new StorageObjectSourceReader(new S3SourceConnectorConfig(settings))
    );
  }
  
  //Visible for testing
  protected S3SourceTask(S3SourceConnectorConfig config, 
      S3Storage storage, StorageObjectSourceReader reader) {
    folders = config.getList(S3SourceConnectorConfig.FOLDERS_CONFIG);
    this.storage = storage;
    this.config = config;
    sourceReader = reader;
    partitioner = config.getPartitioner(storage);
    maxBatchSize = config.getRecordBatchMaxSize();
  }

}
