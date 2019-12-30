/*
 * Copyright [2019 - 2019] Confluent Inc.
 */


package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.StorageObject;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class HDStorageObject implements StorageObject {

  protected FileSystem fileSystem;
  protected String key;
  private static final Logger log = LoggerFactory.getLogger(HDStorageObject.class);
  FSDataInputStream inputStream;

  public HDStorageObject(FileSystem fileSystem, String key) {
    this.fileSystem = fileSystem;
    this.key = key;
    try {
      inputStream = fileSystem.open(new Path(key));
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
  }

  @Override
  public String getKey() {
    try {
      if (fileSystem.exists(new Path(key))) {
        return key;
      }
    } catch (IOException ioException) {
      log.error("Unable to fetch the key {}", key,ioException);
    }
    return "";
  }

  @Override
  public InputStream getObjectContent() {
    return inputStream;
  }

  @Override
  public long getContentLength() {
    try {
      if (fileSystem.exists(new Path(key))) {
        return fileSystem.getLength(new Path(key));
      }
    } catch (IOException ioException) {
      log.error("Unable to return the length of the filesystem {}.", fileSystem, ioException);
    }
    return 0;
  }

  @Override
  public Object getObjectMetadata() {
    try {
      if (fileSystem.exists(new Path(key))) {
        return fileSystem.getLength(new Path(key));
      }
    } catch (IOException ioException) {
      log.error("Unable to return the Metadata of the file {}.", fileSystem, ioException);
    }
    return null;
  }

  @Override
  public void close() {
    try {
      if (fileSystem != null) {
        fileSystem.close();
      }
    } catch (IOException ioException) {
      log.error("Unable to close the filesystem {}.", fileSystem, ioException);
    }
  }
}
