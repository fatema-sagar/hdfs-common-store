/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;


import static java.util.Objects.isNull;
import static org.apache.http.util.TextUtils.isEmpty;

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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;

import io.confluent.connect.gcs.source.GcsSourceConnectorConfig;
import io.confluent.connect.gcs.source.GcsSourceStorage;

/**
 * TimeBased Partitioner class. 
 * It implements Partitioner interface.
 * 
 * <p>folder structure is depends on the partition from 
 * the year, month, day, hour, minutes, and/or seconds</p>
 */
public class TimeBasedPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(TimeBasedPartitioner.class);

  public static final String PATH_FORMAT_CONFIG = "path.format";
  
  private final GcsSourceConnectorConfig config;
  private final GcsSourceStorage gcsStorage;
  private final String extension;
  
  private final DateTimeFormatter formatter;
  private final Map<String, TreeSet<DateTime>> times = new HashMap<>();

  public TimeBasedPartitioner(GcsSourceConnectorConfig config, GcsSourceStorage gcsStorage) {
    this.config = config;
    this.gcsStorage = gcsStorage;
    final String pathFormat = config.getString(PATH_FORMAT_CONFIG);
    this.formatter = DateTimeFormat.forPattern(pathFormat);
    extension = config.getInstantiatedStorageObjectFormat().getExtension();
  }

  @Override
  public Set<String> getPartitions() {
    Set<String> commonPrefixes = calculatePartitions();
    commonPrefixes.forEach(s -> times.put(s, loadDateTimes(s, extension)));
    return commonPrefixes;
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
  
  private Set<String> calculatePartitions() {
    Set<String> commonPrefixes = new HashSet<>();
    List<String> topics = gcsStorage.getListOfBlobs(config.getTopicsFolder(),
            config.getStorageDelimiter());

    topics.forEach(blob -> commonPrefixes.add(blob));

    return commonPrefixes;
  }
  
  TreeSet<DateTime> loadDateTimes(String topic, String extension) {
    String nextPageToken = "";
    TreeSet<DateTime> returnSet = new TreeSet<>();
    do {
      Page<Blob> files = gcsStorage.list(topic, nextPageToken);
      for (Blob blob : files.getValues()) {
        if (blob.getName().startsWith(topic) && blob.getName().endsWith(extension)) {
          returnSet.add(dateFromString(topic, blob.getName()));
        }
      }
      nextPageToken = files.getNextPageToken();
    } while (!isEmpty(nextPageToken));
    log.debug("Got partitions {}", returnSet);
    return returnSet;
  }
  
  DateTime dateFromString(String topic, String key) {
    String temp = key.replaceFirst(Pattern.quote(topic), "");
    String temp2 = temp.substring(0, temp.lastIndexOf(config.getStorageDelimiter()));

    return formatter.parseDateTime(temp2);
  }

  /**
   * It returns the next GCS object name with respect to previous GCS Object name.
   * 
   * <p>For the first time previousObject object name will be null so it gets 
   * the first blob depending upon first value in times map.</p>
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
      return getFirstGcsObjectForADate(topic, times.get(topic).first(), extension);
    }

    String nextFileAlphabetically = "";
    final DateTime next = dateFromString(topic, previousObject);
    String folderPath = topic + formatter.print(next) + config.getStorageDelimiter();
    Blob blobObject = gcsStorage.getNextObject(folderPath, previousObject, extension);

    if (blobObject != null && blobObject.getName().endsWith(extension)) {
      nextFileAlphabetically = blobObject.getName();
    }

    if (theNextObjectIsNotEmptyAndHasTheSameDateAsThePreviousFile(topic,
            previousObject, nextFileAlphabetically)) {
      return nextFileAlphabetically;
    }

    final DateTime nextDate = getNextDate(topic, previousObject);
    return isNull(nextDate) ? ""
                            : getFirstGcsObjectForADate(topic, nextDate, extension);
  }
  
  private String getFirstGcsObjectForADate(String topic, DateTime nextDate, String extension) {
    String folderPath = topic + formatter.print(nextDate) + config.getStorageDelimiter();
    Blob blobObject = gcsStorage.getNextObject(folderPath, null, extension);

    if (blobObject != null && blobObject.getName().endsWith(extension)) {
      return blobObject.getName();
    }
    return "";
  }

  private boolean theNextObjectIsNotEmptyAndHasTheSameDateAsThePreviousFile(
      String topic, String previousObject, String nextFileAlphabetically) {
    return theNextObjectIsNotEmpty(nextFileAlphabetically)
       && thePreviousDateIstheSameAsTheNextDate(topic, previousObject, nextFileAlphabetically);
  }
 
  private boolean theNextObjectIsNotEmpty(String nextFileAlphabetically) {
    return !nextFileAlphabetically.isEmpty();
  }
 
  private boolean thePreviousDateIstheSameAsTheNextDate(String topic, String previousObject, 
      String nextFileAlphabetically) {
    return dateFromString(topic, nextFileAlphabetically)
       .isEqual(dateFromString(topic, previousObject));
  }
  
  private DateTime getNextDate(String topic, String previousObject) {
    NavigableSet<DateTime> set =
        times.get(topic).tailSet(dateFromString(topic, previousObject), false);
    return set.size() != 0 ? set.first() : null;
  }
  
}
