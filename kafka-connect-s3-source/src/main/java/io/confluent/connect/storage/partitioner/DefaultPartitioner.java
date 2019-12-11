/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.confluent.connect.s3.source.S3SourceConnectorConfig;
import io.confluent.connect.s3.source.S3Storage;

/**
 * This class is for Default Partitioner logic and it implements 
 * Partitioner interface.
 * DefaultPartitioner will have folder structure similar to 
 *  - "topics/gcs_topic/partition=0/gcs_topic+0+0000000000.avro".
 */
public class DefaultPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(DefaultPartitioner.class);

  private final S3SourceConnectorConfig config;
  private final S3Storage storage;
  private Set<String> partitions = Collections.emptySet();
  private final String extension;

  public DefaultPartitioner(S3SourceConnectorConfig config, S3Storage storage) {
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
   * "topics/gcs_topic/partition=0/gcs_topic+0+0000000000.json" then 
   * partition value will be "topics/gcs_topic/partition=0/" </p>
   * @return : List of partitions.
   */
  private Set<String> calculatePartitions() {
    Set<String> partitions = new HashSet<>();
    ListObjectsV2Result topics =
        storage.listFolders(config.getTopicsFolder(), config.getStorageDelimiter());
    for (String topic : topics.getCommonPrefixes()) {
      partitions
          .addAll(storage.listFolders(topic, config.getStorageDelimiter()).getCommonPrefixes());
    }

    log.debug("Got partitions {}", partitions);
    return partitions;
  }


  @Override
  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", topic,
        previousObject, extension
    );

    return storage.getNextFileName(topic, previousObject, extension);
  }

  @Override
  public boolean shouldReconfigure() {
    return !partitions.equals(calculatePartitions());
  }
}
