/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.confluent.connect.s3.source.S3SourceConnectorConfig;
import io.confluent.connect.s3.source.S3Storage;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PARTITION_FIELD_NAME_CONFIG;

/**
 * This class is for Field Partitioner logic and it implements 
 * Partitioner interface.
 */
public class FieldPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);

  private final S3SourceConnectorConfig config;
  private final S3Storage storage;
  private Set<String> partitions = Collections.emptySet();
  private final String extension;

  public FieldPartitioner(S3SourceConnectorConfig config, S3Storage storage) {
    this.config = config;
    this.storage = storage;
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  @Override
  public Set<String> getPartitions() {
    partitions = calculatePartitions();
    return partitions;
  }

  private Set<String> calculatePartitions() {
    List<String> topics =
        storage.listFolders(config.getTopicsFolder(), config.getStorageDelimiter())
            .getCommonPrefixes();

    List<String> partitions = recursiveTraverseFolders(topics, config.getFieldNameList().size());

    log.debug("Got partitions {}", partitions);
    return new HashSet<>(partitions);
  }

  private List<String> recursiveTraverseFolders(List<String> pathsSoFar, Integer depth) {
    if (0 == depth) {
      return pathsSoFar;
    } else if (pathsSoFar.isEmpty()) {
      throw new ConnectException(PARTITION_FIELD_NAME_CONFIG
          + " indicated more folders than there were as such is probably incorrectly setup.");
    } else {
      List<String> paths = new ArrayList<>();
      for (String path : pathsSoFar) {
        paths.addAll(storage.listFolders(path, config.getStorageDelimiter()).getCommonPrefixes());
      }


      return recursiveTraverseFolders(paths, depth - 1);
    }
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
