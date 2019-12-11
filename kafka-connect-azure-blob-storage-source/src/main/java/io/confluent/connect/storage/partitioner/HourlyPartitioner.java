/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import java.util.Map;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorConfig;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;

/**
 * HourlyPartitioner class. 
 * It is very similar to TimeBasedPartitioner with only difference in
 * path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
 */
public class HourlyPartitioner extends TimeBasedPartitioner {
  public HourlyPartitioner(AzureBlobStorageSourceConnectorConfig config, 
      AzureBlobSourceStorage storage) {
    super(addFormat(config), storage);
  }

  /**
   * This will add HourlyPartitioner format with 
   * path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
   * i..e each hourly directory will have file/object for one hour.
   *
   * @param config : AzureBlobStorageSourceConnectorConfig object
   * @return : return AzureBlobStorageSourceConnectorConfig object with updated path.format value.
   */
  private static AzureBlobStorageSourceConnectorConfig addFormat(
      AzureBlobStorageSourceConnectorConfig config) {
    Map<String, String> values = (Map<String, String>) config.values();
    String delim = config.getStorageDelimiter();
    String pathFormat =
        "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd" + delim + "'hour'=HH";
    values.put(CloudStorageSourceConnectorCommonConfig.PATH_FORMAT_CONFIG, pathFormat);
    return new AzureBlobStorageSourceConnectorConfig(values);
  }

}
