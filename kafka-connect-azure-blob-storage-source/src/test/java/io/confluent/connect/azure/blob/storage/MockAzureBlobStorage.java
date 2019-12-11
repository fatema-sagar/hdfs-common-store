/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;

import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.StorageException;


public class MockAzureBlobStorage
    extends AzureBlobSourceStorage {

  public MockAzureBlobStorage(AzureBlobStorageSourceConnectorConfig conf,
      String ignoreForConvenienceClass)
      throws URISyntaxException, StorageException, InvalidKeyException, MalformedURLException {
    super(conf, null);
  }

  // Visible for testing.
  public MockAzureBlobStorage(AzureBlobStorageSourceConnectorConfig conf,
      ContainerClient container) {
    super(conf, container);
  }

  Boolean createContainerIfNotExists(ContainerClient containerClient) {
    if (containerClient.exists()) {
      return false;
    } else {
      containerClient.create();
      return true;
    }
  }

  @Override
  public boolean exists(String name) {
    try {
      return isNotBlank(name);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Iterable<BlobItem> list(String path) {
    return new ArrayList<>();
  }

  public boolean bucketExists() {
    return true; 
  }
   
}