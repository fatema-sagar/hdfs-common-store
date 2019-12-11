/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.cloud.storage.source.AbstractCloudStorageSourceConnector;
import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.utils.Version;
import io.confluent.connect.utils.licensing.ConnectLicenseManager;

/**
 * Source connector class for Azure Blob Storage.
 */
public class AzureBlobStorageSourceConnector extends AbstractCloudStorageSourceConnector {

  private static Logger log = LoggerFactory.getLogger(AzureBlobStorageSourceConnector.class);

  private AzureBlobStorageSourceConnectorConfig config;
  private AzureBlobSourceStorage storage;
  protected ConnectLicenseManager licenseManager;

  public AzureBlobStorageSourceConnector() {
  }

  @Override
  public String version() {
    return Version.forClass(this.getClass());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzureBlobStorageSourceTask.class;
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

  @Override
  public ConfigDef config() {
    return AzureBlobStorageSourceConnectorConfig.config();
  }
  
  //Visible for testing
  void setConfig(Map<String, String> props) {
    this.config = new AzureBlobStorageSourceConnectorConfig(props);
    super.config = this.config;
  }
  
  // visible for test
  AzureBlobStorageSourceConnector(
      AzureBlobSourceStorage storage, ConnectLicenseManager licenseManager
  ) {
    super(storage, licenseManager);
  }
  
}