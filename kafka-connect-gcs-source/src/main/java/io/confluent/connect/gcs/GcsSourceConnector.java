/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceConnector;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;
import io.confluent.connect.gcs.source.GcsSourceTask;
import io.confluent.connect.utils.Version;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.gcs.source.GcsSourceConnectorConfig.STORE_URL_CONFIG;

import java.util.Map;

/**
 * Source connector class for Google Cloud Storage.
 */
public class GcsSourceConnector extends AbstractCloudStorageSourceConnector {

  private static Logger log = LoggerFactory.getLogger(GcsSourceConnector.class);

  private GcsSourceConnectorConfig config;

  public GcsSourceConnector() {}

  //visible for testing
  public GcsSourceConnector(GcsSourceStorage storage, ConnectLicenseManager licenseManager) {
    super(storage, licenseManager);
  }

  //visible for testing
  public void setConfig(Map<String, String> props) {
    this.config = new GcsSourceConnectorConfig(props);
    super.config = this.config;
  }

  @Override
  public ConfigDef config() {
    return GcsSourceConnectorConfig.config();
  }

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
      throw new ConnectException("Could not create GcsSourceStorage object.", e);
    }
  }
  
  @Override
  protected Partitioner getPartitioner(CloudSourceStorage storage) {
    return config.getPartitioner(storage);
  }
  
  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return GcsSourceTask.class;
  }

}
