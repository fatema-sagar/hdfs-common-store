/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.cloud.storage.source.CloudSourceStorage;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.hdfs2.Hdfs2SourceConnectorConfig;
import io.confluent.connect.hdfs2.Hdfs2Storage;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Collections;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for Default Partitioner logic and it implements
 * Partitioner interface.
 * DefaultPartitioner will have folder structure similar to
 *  - "topics/hdfs_topic/partition=0/hdfs_topic+0+0000000000+0000000010.avro".
 */
public abstract class DefaultPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(DefaultPartitioner.class);
  private final CloudStorageSourceConnectorCommonConfig config;
  private final CloudSourceStorage storage;
  private Set<String> partitions = Collections.emptySet();
  private final String extension;

  public DefaultPartitioner(CloudStorageSourceConnectorCommonConfig config, CloudSourceStorage storage) {
    this.config = config;
    this.storage = createStorage();;
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  public Set<String> getPartitions() {
    partitions = calculatePartitions();
    return partitions;
  }

  @Override
  public String getNextObjectName(String topic, String previousObject) {
//    List<FileStatus> fileStatusList = storage.checkForFileExtension(storage.list(topic),extension);
//    log.info("get the next file from the path {}" , topic);
//    return storage.getNextFile(fileStatusList , previousObject);
     return storage.getNextObject(topic, previousObject);
  }

  /**
   * Calculates number of partitions from given topic folder.
   * <p> Example : if file is located at -
   * "topics/hdfs_topic/partition=0/hdfs_topic+0+0000000000+0000000010.json" then
   * partition value will be "topics/hdfs_topic/partition=0/" </p>
   * @return : List of partitions.
   */
  private Set<String> calculatePartitions() {
//    TreeSet<String> partitions = new TreeSet<>();
//    List<FileStatus> filterTopics = new ArrayList<>();
//    List<FileStatus> topics = storage.list(
//            config.getHdfsUrl() + config.getStorageDelimiter() + config.getTopicsFolder());
//    if (topics == null) {
//      return partitions;
//    }
//    for (FileStatus topic : topics) {
//      if (!topic.getPath().toString().contains("+tmp")) {
//        filterTopics.add(topic);
//      }
//    }
//    //ignore the +tmp directory which contains not required files
//    for (FileStatus file: filterTopics) {
//      List<FileStatus> innerFileStatus = storage.list(file.getPath().toString());
//      for (FileStatus inner : innerFileStatus) {
//        partitions.add(inner.getPath().toString());
//      }
//    }
//    log.debug("Got partitions {}", partitions);
//    return partitions;
    return storage.getPartitionList();
  }

  public boolean shouldReconfigure() {
    return !partitions.equals(calculatePartitions());
  }
}
