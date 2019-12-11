/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.util;

import org.apache.kafka.connect.errors.ConnectException;

import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;
import io.confluent.connect.cloud.storage.source.StorageObject;
import io.confluent.connect.cloud.storage.source.StorageObjectFormat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A Source Reader class to call for right Object Formatter 
 * to process StorageObject.
 */
public class StorageObjectSourceReader {

  private final StorageObjectFormat converter;
  private final String directoryDelimiter;
  private final String fileNameRegexPattern;

  public StorageObjectSourceReader(CloudStorageSourceConnectorCommonConfig config) 
      throws ConnectException {
    converter = config.getInstantiatedStorageObjectFormat();
    directoryDelimiter = config.getStorageDelimiter();
    fileNameRegexPattern = config.getString(
        CloudStorageSourceConnectorCommonConfig.FILE_NAME_REGEX_PATTERN);
  }

  public SourceRecordOffset nextRecord(StorageObject object, String folder, long offset) {
    return converter.nextRecord(object, folder, directoryDelimiter, offset, fileNameRegexPattern);
  }

  public void skip(StorageObject object, long offset) {
    converter.skip(object.getObjectContent(), offset);
  }

  public static Map<String, String> buildSourcePartition(String folder) {
    return Collections.singletonMap("folder", folder);
  }

  public static Map<String, String> buildSourceOffset(String filename, long offset, boolean eof) {
    final Map<String, String> sourceOffset = new HashMap<>();
    sourceOffset.put("lastFileRead", filename);
    sourceOffset.put("fileOffset", String.valueOf(offset));
    sourceOffset.put("eof", String.valueOf(eof));
    return sourceOffset;
  }
}
