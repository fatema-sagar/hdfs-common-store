/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorConfig;

/**
 * This class is for Default Partitioner logic and it implements 
 * Partitioner interface.
 * DefaultPartitioner will have folder structure similar to 
 *  - "topics/abs_topic/partition=0/abs_topic+0+0000000000.avro".
 */
public class DefaultPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(DefaultPartitioner.class);

  private final AzureBlobStorageSourceConnectorConfig config;
  private final AzureBlobSourceStorage storage;
  private Set<String> partitions = Collections.emptySet();
  private final String extension;

  public DefaultPartitioner(
      AzureBlobStorageSourceConnectorConfig config, AzureBlobSourceStorage storage) {
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
   * "topics/abs_topic/partition=0/abs_topic+0+0000000000.json" then 
   * partition value will be "topics/abs_topic/partition=0/" </p>
   * @return : List of partitions.
   */
  private Set<String> calculatePartitions() {
    Set<String> partitions = storage
        .listFolders(config.getTopicsFolder(), config.getStorageDelimiter());
    
    log.debug("Got partitions {}", partitions);
    return partitions;
  }

  @Override
  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} "
        + "and extension {}", topic, previousObject, extension
    );
    return storage.getNextFileName(topic, previousObject, extension);
  }

  @Override
  public boolean shouldReconfigure() {
    return !partitions.equals(calculatePartitions());
  }
}
