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
    log.info("Recieved partitions"+partitions);
    return partitions;
  }

  /**
   * Calculates number of partitions from given topic folder.
   * <p> Example : if file is located at -
   * "hdfs://localhost:9000/topics/hdfs_topic/partition=0/hdfs_topic+0+0000000000.avro" then
   * partition value will be "topics/hdfs_topic/partition=0/" </p>
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

  /**
   * This method is responsible to return the next Filename/Object which has to be read by the Kafka.
   * @param topic The Topic name
   * @param previousObject denotes the file already read by Kafka.
   * @return the name of the file which has to be read next by Kafka.
   */

  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", topic,
            previousObject, extension
    );
    String fileName = storage.getNextFileName(topic, previousObject, extension);
    return fileName;
  }

  public boolean shouldReconfigure() {
    return !partitions.equals((calculatePartitions()));
  }
}
