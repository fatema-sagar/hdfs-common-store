/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.hdfs.source.HDSourceConnectorConfig;
import io.confluent.connect.hdfs.source.HDStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultPartitioner implements Partitioner {
  private static final Logger log = LoggerFactory.getLogger(DefaultPartitioner.class);

  private final HDSourceConnectorConfig config;
  private final HDStorage storage;
  private Set<String> partitions = Collections.emptySet();
  private final String extension;

  public DefaultPartitioner(HDSourceConnectorConfig config, HDStorage storage) {
    this.config = config;
    this.storage = storage;
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  @Override
  public Set<String> getPartitions() {
    partitions = calculatePartitions();
    return partitions;
  }

  /**
   * Calculates number of partitions from given topic folder.
   * <p> Example : if file is located at -
   * "topics/hdfs_topic/partition=0/hdfs_topic+0+0000000000.avro" then
   * partition value will be "topics/gcs_topic/partition=0/" </p>
   * @return : List of partitions.
   */
  private Set<String> calculatePartitions() {
    Set<String> partitions = new HashSet<>();
    try {
      partitions = storage.readFiles();
    } catch (IOException e) {
      e.printStackTrace();
    }
    log.debug("Got partitions {}", partitions);
    return partitions;
  }

  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", topic,
            previousObject, extension
    );
    return storage.getNextFileName(topic, previousObject, extension);
  }

  public boolean shouldReconfigure() {
    return !partitions.equals((calculatePartitions()));
  }
}
