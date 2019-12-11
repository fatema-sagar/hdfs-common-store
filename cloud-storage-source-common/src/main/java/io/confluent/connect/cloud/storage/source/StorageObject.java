/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import java.io.InputStream;

/**
 * This interface will have methods needed to process storage objects 
 * retrieved from cloud storage.
 */
public interface StorageObject {
  
  /**
   * get key of given storage object.
   * @return : name or key value of storage object.
   */
  String getKey();
  
  /**
   * get object content for given storage object.
   * @return : InputStream of storage object.
   */
  InputStream getObjectContent();
  
  /**
   * get content length of storage object.
   * @return : long value of content length of storage object.
   */
  long getContentLength();
  
  /**
   * get metadata of given storage object.
   * @return : object containing Metadata.
   */
  Object getObjectMetadata();
  
  /**
   * Close storage object or stream.
   */
  void close();

}
