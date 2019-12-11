/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.confluent.connect.s3.source.S3SourceConnectorConfig;
import io.confluent.connect.s3.source.S3Storage;

import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PATH_FORMAT_CONFIG;
import static java.util.Objects.isNull;
import static org.apache.http.util.TextUtils.isEmpty;

public class TimeBasedPartitioner implements Partitioner {

  private static final Logger log = LoggerFactory.getLogger(TimeBasedPartitioner.class);

  private final S3SourceConnectorConfig config;
  private final S3Storage storage;
  private final DateTimeFormatter formatter;
  private final Map<String, TreeSet<DateTime>> times = new HashMap<>();
  private final String extension;

  public TimeBasedPartitioner(S3SourceConnectorConfig config, S3Storage storage) {
    this.config = config;
    this.storage = storage;
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

  private Set<String> calculatePartitions() {
    ListObjectsV2Result topics =
        storage.listFolders(config.getTopicsFolder(), config.getStorageDelimiter());

    log.debug("Got partitions {}", topics.getCommonPrefixes());
    return new HashSet<>(topics.getCommonPrefixes());
  }

  @Override
  public String getNextObjectName(String topic, String previousObject) {
    log.trace("Get next object from topic {} using previous object {} and extension {}", topic,
        previousObject, extension
    );

    if (!times.containsKey(topic)) {
      times.put(topic, loadDateTimes(topic, extension));
    }

    if (isEmpty(previousObject)) {
      return getFirstS3ObjectForADate(topic, times.get(topic).first(), extension);
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
                            : getFirstS3ObjectForADate(topic, nextDate, extension);
  }

  private String getFirstS3ObjectForADate(
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
    String continuationToken = null;
    TreeSet<DateTime> returnSet = new TreeSet<>();
    do {
      ListObjectsV2Result files = storage.listFiles(topic, continuationToken);
      for (S3ObjectSummary key : files.getObjectSummaries()) {
        if (key.getKey().startsWith(topic) && key.getKey().endsWith(extension)) {
          returnSet.add(dateFromString(topic, key.getKey()));
        }
      }
      continuationToken = files.getContinuationToken();
    } while (!isEmpty(continuationToken));
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
}
