/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import java.io.Closeable;
import java.util.List;

/**
 * <p>This is SourceStorage interface which have methods needed to read 
 * data/object from a cloud storage.
 * 
 * Every cloud storage source connector has to implement this interface to 
 * retrieve storage object.
 * 
 * In case of S3 source connect, S3Storage.java class has to implement it.</p>
 */
public interface CloudSourceStorage extends Closeable {

  /**
   * Get storage Object for given key path.
   * @param key : key or path of the storage object.
   * @return : Object of StorageObject.
   */
  StorageObject getStorageObject(String key);
  
  /**
   * Get list of storage objects for given path.
   * @param path : path of the storage objects.
   * @return : list of StorageObject.
   */
  List<StorageObject> getListOfStorageObjects(String path);
  
  /**
   * Check it storage object exists for given name/key.
   * @param name : name of storage object
   * @return : true if object exists else false.
   */
  boolean exists(String name);
  
  /**
   * Check if bucket exists.
   * @return : true if bucket exists else false.
   */
  boolean bucketExists();
  
  /**
   * Delete a given object.
   * @param name : name/key of storage object to be deleted.
   */
  void delete(String name);
  
  /**
   * Return StorageObject for given path.
   * @param path : path for StorageObject
   * @return : StorageObject.
   */
  StorageObject open(String path);

  default void close() {
  }
  
}
