/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.source.TestWithMockedS3;

import static io.confluent.connect.s3.source.S3AvroTestUtils.writeAvroFile;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PARTITIONER_CLASS_CONFIG;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PATH_FORMAT_CONFIG;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TimeBasedPartitionerTest extends TestWithMockedS3 {

  public static final String OTHER_S3_TEST_FOLDER_TOPIC_NAME = "topics/other_s3_bucket";

  @Before
  public void setup() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(PARTITIONER_CLASS_CONFIG, TimeBasedPartitioner.class.getCanonicalName());
    props.put(PATH_FORMAT_CONFIG, "'second'=ss/'month'=MM/'day'=dd/'hour'=HH/'year'=YYYY");
    return props;
  }

  @Test
  public void get1PartitionHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME
            + "/second=45/month=12/day=01/hour=23/year=2015/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    assertThat(partitioner.getPartitions().size(), is(1));
    assertThat(partitioner.getPartitions().iterator().next(), is(S3_TEST_FOLDER_TOPIC_NAME + "/"));
  }

  @Test
  public void get2PartitionsHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME
            + "/second=34/month=12/day=01/hour=12/year=2015/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        OTHER_S3_TEST_FOLDER_TOPIC_NAME
            + "/second=34/month=12/day=01/hour=12/year=2015/test-topic+0+0000000000.avro",
        avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    assertThat(partitioner.getPartitions().size(), is(2));
    final Iterator<String> iterator = partitioner.getPartitions().iterator();
    assertThat(iterator.next(), is(S3_TEST_FOLDER_TOPIC_NAME + "/"));
    assertThat(iterator.next(), is(OTHER_S3_TEST_FOLDER_TOPIC_NAME + "/"));
  }

  @Test
  public void getFirstFile() throws IOException {
    String folder = "/second=12/month=03/day=07/hour=22/year=1945/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), "");
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro")
    );
  }

  @Test
  public void getSecondFileSameFolder() throws IOException {
    String folder = "/second=12/month=04/day=09/hour=04/year=1980/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000001.avro", avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), "");
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro")
    );
    String secondObject = partitioner
        .getNextObjectName(
            partitioner.getPartitions().iterator().next(),
            firstObject
        );
    assertNotNull(secondObject);
    assertThat(
        secondObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000001.avro")
    );
  }

  @Test
  public void getSecondFileDifferentFolder() throws IOException {
    String folder = "/second=51/month=11/day=02/hour=23/year=2011/";
    String folder2 = "/second=32/month=02/day=04/hour=19/year=2016/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        OTHER_S3_TEST_FOLDER_TOPIC_NAME + folder2 + "test-topic+0+0000000000.avro",
        avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), "");
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro")
    );
    String secondObject = partitioner
        .getNextObjectName(
            partitioner.getPartitions().iterator().next(),
            firstObject
        );
    assertNotNull(secondObject);
  }

  @Test
  public void getSecondFileWhichDoesntExist() throws IOException {
    String folder = "/second=23/month=10/day=02/hour=14/year=2001/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );

    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof TimeBasedPartitioner);
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), "");
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro")
    );
    String secondObject = partitioner
        .getNextObjectName(
            partitioner.getPartitions().iterator().next(),
            firstObject
        );
    assertNotNull(secondObject);
  }

  @Test
  public void shouldConvertObjectKeysToDates() throws IOException {
    // Note that day will change with each object so they are different
    String folder = "/second=12/month=03/day=0%d/hour=22/year=1945/"; 
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);

    final int files = 3;
    for (int i = 0; i < files; i++) {
      s3.putObject(S3_TEST_BUCKET_NAME,
          format(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+000000000%d.avro", i + 1, i),
          avroFile.toFile()
      );
    }

    TimeBasedPartitioner partitioner =
        (TimeBasedPartitioner) connectorConfig.getPartitioner(storage);

    String topic = partitioner.getPartitions().iterator().next();
    TreeSet<DateTime> dateTimes = partitioner.loadDateTimes(
        topic,
        AvroFormat.DEFAULT_AVRO_EXTENSION
    );

    // Should have 3 datetimes - from 3 objects - each with an incrementing day value starting at 1
    assertThat(dateTimes.size(), is(3));
    assertThat(dateTimes, hasItem(new DateTime(1945, 3, 1, 22, 0, 12)));
    assertThat(dateTimes, hasItem(new DateTime(1945, 3, 2, 22, 0, 12)));
    assertThat(dateTimes, hasItem(new DateTime(1945, 3, 3, 22, 0, 12)));
  }

  @Test
  public void shouldConvertS3ObjectKeyToDate() {
    TimeBasedPartitioner partitioner =
        (TimeBasedPartitioner) connectorConfig.getPartitioner(storage);

    String topic = "topics/test-topic/";
    DateTime dateTime = partitioner.dateFromString(
        topic,
        topic + "second=45/month=12/day=01/hour=23/year=2015/test-topic+0+0000000000.avro"
    );

    assertThat(dateTime.getYear(), is(2015));
    assertThat(dateTime.getMonthOfYear(), is(12));
    assertThat(dateTime.getDayOfMonth(), is(1));
    assertThat(dateTime.getHourOfDay(), is(23));
    assertThat(dateTime.getSecondOfMinute(), is(45));
  }

  @Test
  public void shouldHandleMultipleFolders() throws IOException {
    String firstFolderPrefix = S3_TEST_FOLDER_TOPIC_NAME + "/";
    String secondFolderPrefix = S3_TEST_FOLDER_TOPIC_NAME + "2/";
    String folder = "second=12/month=03/day=07/hour=22/year=1945/";

    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        firstFolderPrefix + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    s3.putObject(S3_TEST_BUCKET_NAME,
        secondFolderPrefix + folder + "test-topic-2+0+0000000000.avro", avroFile.toFile()
    );

    TimeBasedPartitioner partitioner =
        (TimeBasedPartitioner) connectorConfig.getPartitioner(storage);

    String firstObject =
        partitioner.getNextObjectName(firstFolderPrefix, null);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(firstFolderPrefix + folder + "test-topic+0+0000000000.avro")
    );

    String secondObject =
        partitioner.getNextObjectName(secondFolderPrefix, null);
    assertNotNull(secondObject);
    assertThat(
        secondObject,
        is(secondFolderPrefix + folder + "test-topic-2+0+0000000000.avro")
    );
  }
}
