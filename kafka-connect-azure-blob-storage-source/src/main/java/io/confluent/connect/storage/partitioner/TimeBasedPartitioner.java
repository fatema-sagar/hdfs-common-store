/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import static java.util.Objects.isNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.models.BlobItem;

import io.confluent.connect.azure.blob.storage.AzureBlobSourceStorage;
import io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnectorConfig;
import io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig;

/**
 * TimeBased Partitioner class. 
 * It implements Partitioner interface.
 * 
 * <p>folder structure is depends on the partition from 
 * the year, month, day, hour, minutes, and/or seconds</p>
 */
public class TimeBasedPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(TimeBasedPartitioner.class);

  private final AzureBlobStorageSourceConnectorConfig config;
  private final AzureBlobSourceStorage storage;
  private final DateTimeFormatter formatter;
  private final Map<String, TreeSet<DateTime>> times = new HashMap<>();
  private final String extension;

  public TimeBasedPartitioner(AzureBlobStorageSourceConnectorConfig config, 
      AzureBlobSourceStorage storage) {
    this.config = config;
    this.storage = storage;
    final String pathFormat = config.getString(
        CloudStorageSourceConnectorCommonConfig.PATH_FORMAT_CONFIG);
    this.formatter = DateTimeFormat.forPattern(pathFormat);
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  @Override
  public Set<String> getPartitions() {
    Set<String> commonPrefixes = calculatePartitions();
    commonPrefixes.forEach(s -> times.put(s, loadDateTimes(s, extension)));
    return commonPrefixes;
  }

  private Set<String> calculatePartitions() {
    Set<String> topics = new HashSet<>(
        storage.listBaseFolders(config.getTopicsFolder(), config.getStorageDelimiter()));

    log.debug("Got partitions {}", topics);
    return topics;
  }

  /**
   * It returns the next AzureBlobStorage object name with respect to previous 
   * AzureBlobStorage Object name.
   * 
   * <p>For the first time previousObject object name will be null so it gets 
   * the first blobItem depending upon first value in times map.</p>
   */
  @Override
  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", topic,
        previousObject, extension
    );

    if (!times.containsKey(topic)) {
      times.put(topic, loadDateTimes(topic, extension));
    }

    if (isEmpty(previousObject)) {
      return getFirstAzureBlobObjectForADate(topic, times.get(topic).first(), extension);
    }

    final String nextFileAlphabetically =
        storage.getNextFileName(topic, previousObject, extension);

    if (theNextObjectIsNotEmptyAndHasTheSameDateAsThePreviousFile(topic, previousObject,
        nextFileAlphabetically
    )) {
      return nextFileAlphabetically;
    }

    final DateTime nextDate = getNextDate(topic, previousObject);
    return isNull(nextDate) ? ""
                            : getFirstAzureBlobObjectForADate(topic, nextDate, extension);
  }

  private String getFirstAzureBlobObjectForADate(
      String topic, DateTime nextDate, String extension
  ) {
    return storage.getNextFileName(topic + formatter.print(nextDate), null, extension);
  }

  private DateTime getNextDate(String topic, String previousObject) {
    NavigableSet<DateTime> set =
        times.get(topic).tailSet(dateFromString(topic, previousObject), false);
    return set.size() != 0 ? set.first() : null;
  }

  private boolean theNextObjectIsNotEmptyAndHasTheSameDateAsThePreviousFile(
      String topic, String previousObject, String nextFileAlphabetically
  ) {
    return theNextObjectIsNotEmpty(nextFileAlphabetically)
        && thePreviousDateIstheSameAsTheNextDate(topic, previousObject, nextFileAlphabetically);
  }

  private boolean thePreviousDateIstheSameAsTheNextDate(
      String topic, String previousObject, String nextFileAlphabetically
  ) {
    return dateFromString(topic, nextFileAlphabetically)
        .isEqual(dateFromString(topic, previousObject));
  }

  private boolean theNextObjectIsNotEmpty(String nextFileAlphabetically) {
    return !nextFileAlphabetically.isEmpty();
  }

  TreeSet<DateTime> loadDateTimes(String topic, String extension) {
    TreeSet<DateTime> returnSet = new TreeSet<>();     
    List<BlobItem> blobSegmentResponse = storage
        .getBlobsListStreamResponse(topic); 
    for (BlobItem blobItem : blobSegmentResponse) {
      if (blobItem.name().startsWith(topic) && blobItem.name().endsWith(extension)) {
        returnSet.add(dateFromString(topic, blobItem.name())); 
      } 
    } 
    return returnSet;
  }

  DateTime dateFromString(String topic, String key) {
    String temp = key.replaceFirst(Pattern.quote(topic), "");
    String temp2 = temp.substring(0, temp.lastIndexOf(config.getStorageDelimiter()));

    return formatter.parseDateTime(temp2);
  }

  @Override
  public boolean shouldReconfigure() {
    return haveDatesChanged();
  }

  private boolean haveDatesChanged() {
    Map<String, TreeSet<DateTime>> tempTimes = calculatePartitions().stream()
        .collect(Collectors.toMap(Object::toString, s -> loadDateTimes(s, extension)));
    return !times.equals(tempTimes);
  }
  
  public static boolean isEmpty(final CharSequence charSequence) {
    if (charSequence == null) {
      return true;
    }
    return charSequence.length() == 0;
  }

}
