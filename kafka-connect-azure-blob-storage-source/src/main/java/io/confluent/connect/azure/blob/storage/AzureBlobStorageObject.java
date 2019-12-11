/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage;

import static java.util.Objects.isNull;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlockBlobClient;
import com.azure.storage.blob.models.BlobItem;

import io.confluent.connect.cloud.storage.source.StorageObject;

/**
 * Wrapper class for Azure Blob Storage object.
 */
public class AzureBlobStorageObject implements StorageObject {

  private static final Logger log = LoggerFactory.getLogger(AzureBlobStorageObject.class);
  
  private final long lengthZero = 0L;

  private BlobItem delegate;
  private InputStream dataStream = null;
  
  public AzureBlobStorageObject(BlobItem blobItem, AzureBlobSourceStorage storage) {
    this.delegate = blobItem;
    if (!isAzureBlobStorageEmpty()) {
      /*
       * Create a client that references a to-be-created blob in your Azure Storage account's 
       * container. This returns a BlockBlobClient object that wraps the blob's endpoint, 
       * credential and a request pipeline (inherited from containerClient). 
       * Note that blob names can be mixed case.
       */
      final BlockBlobClient blobClient = storage.getAzureContinerClient()
          .getBlobClient(blobItem.name()).asBlockBlobClient();
      
      this.dataStream = blobClient.openInputStream();
    }
  }
  
  /**
   * Overriding the parent method to get the name of the AzureBlobStorage Object
   * 
   * @return String the name of blobItem
   */
  @Override
  public String getKey() {
    return delegate.name();
  }

  /**
   * Overriding the parent method to get the content of the AzureBlobStorage
   * Object in input stream format
   * 
   * @return InputStream the content of blobItem
   */
  @Override
  public InputStream getObjectContent() {
    if (!isAzureBlobStorageEmpty()) {
      return this.dataStream;
    }
    return null;
  }
  
  /**
   * Overriding the parent method to get the content length of the 
   * AzureBlobStorage Object in input stream format
   * 
   * @return long the content length of blobItem
   */
  @Override
  public long getContentLength() {
    if (!isAzureBlobStorageEmpty()) {
      return delegate.properties().contentLength();
    }
    return 0;
  }

  /**
   * To check if the AzureBlobStorage Object is empty
   * 
   * @return boolean status mentioning if the AzureBlobStorage Object is empty or not
   */
  private boolean isAzureBlobStorageEmpty() {
    return isNull(delegate.properties().contentLength())
        || delegate.properties().contentLength() == lengthZero;
  }

  @Override
  public void close() {
    try {
      dataStream.close();
    } catch (Exception e) {
      log.error("Error closing the ByteArrayInputStream for the blobItem {}:", 
          delegate.name());
    }
  }

  /**
   * Overriding the parent method to get the metadata of the AzureBlobStorage 
   * Object in input stream format
   * 
   * @return Object the object metadata of blobItem
   */
  @Override
  public Object getObjectMetadata() {
    if (!isAzureBlobStorageEmpty()) {
      return delegate.properties().contentLength();
    }
    return null;
  }
  
}
