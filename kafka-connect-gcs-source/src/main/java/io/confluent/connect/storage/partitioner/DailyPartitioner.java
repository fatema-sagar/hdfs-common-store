/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;

/**
 * DailyPartitioner class. 
 * It is very similar to TimeBasedPartitioner with only difference in
 * path.format='year'=YYYY/'month'=MM/'day'=dd
 */
public class DailyPartitioner extends TimeBasedPartitioner {

  public DailyPartitioner(GcsSourceConnectorConfig config, GcsSourceStorage storage) {
    super(addFormat(config), storage);
  }

  /**
   * This will add daily partitioner format with path.format='year'=YYYY/'month'=MM/'day'=dd
   * i..e each daily directory will have file/object for one day.
   *
   * @param config : GcsSourceConnectorConfig object
   * @return : return GcsSourceConnectorConfig object with updated path.format value.
   */
  @SuppressWarnings("unchecked")
  private static GcsSourceConnectorConfig addFormat(GcsSourceConnectorConfig config) {
    Map<String, String> values = new HashMap<>((Map<String, String>) config.values());
    String delim = config.getStorageDelimiter();
    String pathFormat = "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd";
    values.put(PATH_FORMAT_CONFIG, pathFormat);
    return new GcsSourceConnectorConfig(values);
  }

}
