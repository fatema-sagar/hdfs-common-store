/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is to process topic and partition name from file path or file key.
 */
public class StorageObjectKeyParts {
  
  private static final Logger log = LoggerFactory.getLogger(StorageObjectKeyParts.class);

  private final String topic;
  private final Integer partition;

  private StorageObjectKeyParts(String topic, Integer partition) {
    this.topic = topic;
    this.partition = partition;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public static Optional<StorageObjectKeyParts> fromKey(String directoryDelimiter, 
      String key, String fileNameRegexPattern) {
    String delim = Pattern.quote(directoryDelimiter);
    Pattern p = Pattern.compile(".*" + delim + fileNameRegexPattern);
    Matcher m = p.matcher(key);
    if (m.matches()) {
      String topic = m.group(1);
      int partition = Integer.parseInt(m.group(2));

      log.trace("Extracted topic {} and partition {} from key {}", topic, partition, key);
      return Optional.of(new StorageObjectKeyParts(topic, partition));
    }

    log.error("Could not extract topic and partition from key {}", key);
    return Optional.empty();
  }
}
