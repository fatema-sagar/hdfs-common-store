/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceConnector;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.utils.Version;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.STORE_URL_CONFIG;

import java.util.Map;

/**
 * Source connector class for S3.
 */
public class S3SourceConnector extends AbstractCloudStorageSourceConnector {

  public static final int MAX_TIMEOUT = 10;

  private static Logger log = LoggerFactory.getLogger(S3SourceConnector.class);

  private S3SourceConnectorConfig config;

  public S3SourceConnector() {}

  //visible for test
  S3SourceConnector(S3Storage storage, ConnectLicenseManager licenseManager) {
    super(storage, licenseManager);
  }
  
  @Override
  protected CloudStorageSourceConnectorCommonConfig createConfig(Map<String, String> props) {
    this.config = new S3SourceConnectorConfig(props);
    return this.config;
  }

  @Override
  protected CloudSourceStorage createStorage() {
    return new S3Storage(config, config.getString(STORE_URL_CONFIG));
  }

  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }
  
  void setConfig(Map<String, String> props) {
    this.config = new S3SourceConnectorConfig(props);
    super.config = this.config;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return S3SourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return S3SourceConnectorConfig.config();
  }
  
  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

}
