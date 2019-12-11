/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorConfig;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;

/**
 * DailyPartitioner class. 
 * It is very similar to TimeBasedPartitioner with only difference in
 * path.format='year'=YYYY/'month'=MM/'day'=dd.
 */
public class DailyPartitioner extends TimeBasedPartitioner {
  public DailyPartitioner(AzureBlobStorageSourceConnectorConfig config, 
      AzureBlobSourceStorage storage) {
    super(addFormat(config), storage);
  }

  /**
   * This will add daily partitioner format with path.format='year'=YYYY/'month'=MM/'day'=dd
   * i.e each daily directory will have file/object for one day.
   *
   * @param config : AzureBlobStorageSourceConnectorConfig object
   * @return : return AzureBlobStorageSourceConnectorConfig object with updated path.format value.
   */
  private static AzureBlobStorageSourceConnectorConfig addFormat(
      AzureBlobStorageSourceConnectorConfig config) {
    Map<String, String> values = new HashMap<>((Map<String, String>) config.values());
    String delim = config.getStorageDelimiter();
    String pathFormat = "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd";
    values.put(CloudStorageSourceConnectorCommonConfig.PATH_FORMAT_CONFIG, pathFormat);
    return new AzureBlobStorageSourceConnectorConfig(values);
  }

}
