/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.Map;

import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;

/**
 * HourlyPartitioner class. 
 * It is very similar to TimeBasedPartitioner with only difference in
 * path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
 */
public class HourlyPartitioner  extends TimeBasedPartitioner {
  
  public HourlyPartitioner(GcsSourceConnectorConfig config, GcsSourceStorage storage) {
    super(addFormat(config), storage);
  }

  /**
   * This will add HourlyPartitioner format with 
   * path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
   * i..e each hourly directory will have file/object for one hour.
   *
   * @param config : GcsSourceConnectorConfig object
   * @return : return GcsSourceConnectorConfig object with updated path.format value.
   */
  @SuppressWarnings("unchecked")
  private static GcsSourceConnectorConfig addFormat(GcsSourceConnectorConfig config) {
    Map<String, String> values = (Map<String, String>) config.values();
    String delim = config.getStorageDelimiter();
    String pathFormat =
        "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd" + delim + "'hour'=HH";
    values.put(PATH_FORMAT_CONFIG, pathFormat);
    return new GcsSourceConnectorConfig(values);
  }
}
