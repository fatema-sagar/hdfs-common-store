/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.hdfs.source;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.StorageObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * This has implementation of methods needed to interact with
 * Hadoop File System.
 */
public class HDStorage implements CloudSourceStorage {
  private static final Logger log = LoggerFactory.getLogger(HDStorage.class);
  private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaHDFSConnector/%s";
  private final String url;
  private String key;
  private final HDSourceConnectorConfig config;
  private FileSystem fileSystem;
  private Set<String> topicPartiton = new HashSet<>();

  /**
   * @param url    HDFS address
   * @param config HDFS Configuration
   */
  public HDStorage(HDSourceConnectorConfig config, String url) throws IOException {
    this.url = url;
    this.config = config;
    fileSystem =  FileSystem.newInstance(URI.create(config.getHdfsUrl()), new Configuration());
  }

  public String getUrl() {
    return url;
  }

  public HDSourceConnectorConfig getConfig() {
    return config;
  }

  /**
   * @throws IOException The exception is thrown when the connector is unable to find the url.
   */
  public Set<String> readFiles() throws IOException {
    FileStatus[] fileStatus = fileSystem.listStatus(new Path(config.getHdfsUrl() + config.getTopicsFolder() + config.getStorageDelimiter()));
    for (FileStatus status : fileStatus) {
      readFilesFromFolder(status.getPath().toString());
    }
    return topicPartiton;
  }

  private void readFilesFromFolder(String path) throws IOException {
    FileStatus[] status = fileSystem.listStatus(new Path(path));
    for (FileStatus fileStatus : status) {
      if (!fileStatus.getPath().toString().contains("/+tmp")) {
        log.info("Partitions: " + status);
        topicPartiton.add(fileStatus.getPath().toString());
      }
    }
  }

  @Override
  public StorageObject getStorageObject(String key) {
    this.key = key;
    return new HDStorageObject(fileSystem, key);
  }

  @Override
  public List<StorageObject> getListOfStorageObjects(String path) {
    throw new UnsupportedOperationException("Implementation not added in HDFS Storage class.");
  }

  @Override
  public boolean exists(String name) {
    try {
      if (fileSystem.exists(new Path(name))) {
        return true;
      }
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean bucketExists() {
    try {
      if (fileSystem.exists(new Path(config.getHdfsUrl() + config.getTopicsFolder()))) {
        return true;
      }
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
    return false;
  }

  @Override
  public void delete(String name) {
    try {
      if (fileSystem.exists(new Path(name))) {
        fileSystem.delete(new Path(name));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public StorageObject open(String path) {
    log.trace("Opening file stream to path {}", path);
    return new HDStorageObject(fileSystem, path);
  }

//  public void close() {
//    try {
//      //fileSystem.close();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }

  public String getNextFileName(String path, String startAfterThisFile, String fileSuffix) {
    log.trace("Listing objects on hdfs with path {} starting after file {} and having suffix {}",
            path, startAfterThisFile, fileSuffix
    );
    String startAfterThis = startAfterThisFile;
    String filename = "";
    try {
      List<FileStatus> fileStatuses = Arrays.asList(fileSystem.listStatus(new Path(path)));
      FileStatus status;
      if (fileStatuses == null) {
        log.info("Partition in the given path {} is empty.", path);
        return "";
      }
      if (fileStatuses.size() == 1) {
        filename = fileStatuses.get(0).getPath().toString();
        return filename;
      }
      Iterator iterator = fileStatuses.iterator();
      while (iterator.hasNext()) {
        if (startAfterThis == null) {
          status = (FileStatus) iterator.next();
          return status.getPath().toString();
        } else {
          if (iterator.next().toString().contains(startAfterThisFile)) {
            startAfterThis = null;
            continue;
          }
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return filename;
  }
}
