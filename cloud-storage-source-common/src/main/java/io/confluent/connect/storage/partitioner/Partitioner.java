/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.Set;

/**
 * Partitioner interface that needs to implemented by various Partitioner class.
 * Ex:- DefaultPartitioner, FieldPartitioner etc.
 */
public interface Partitioner {

  Set<String> getPartitions();

  String getNextObjectName(String topic, String previousObject);

  boolean shouldReconfigure();

}
