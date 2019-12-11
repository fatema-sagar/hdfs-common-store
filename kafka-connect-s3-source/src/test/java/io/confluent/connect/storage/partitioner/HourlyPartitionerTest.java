/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import io.confluent.connect.s3.source.TestWithMockedS3;

import static io.confluent.connect.s3.source.S3AvroTestUtils.writeAvroFile;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PARTITIONER_CLASS_CONFIG;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HourlyPartitionerTest extends TestWithMockedS3 {

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
    props.put(PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getCanonicalName());
    return props;
  }

  @Test
  public void get1PartitionHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME
            + "/year=2015/month=12/day=01/hour=23/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof HourlyPartitioner);
    assertThat(partitioner.getPartitions().size(), is(1));
    assertThat(partitioner.getPartitions().iterator().next(), is(S3_TEST_FOLDER_TOPIC_NAME + "/"));
  }

  @Test
  public void get2PartitionsHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME
            + "/year=2015/month=12/day=01/hour=12/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(S3_TEST_BUCKET_NAME, OTHER_S3_TEST_FOLDER_TOPIC_NAME
        + "/year=2016/month=02/day=04/hour=14/test-topic+0+0000000000.avro", avroFile2.toFile());
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof HourlyPartitioner);
    assertThat(partitioner.getPartitions().size(), is(2));
    final Iterator<String> iterator = partitioner.getPartitions().iterator();
    assertThat(iterator.next(), is(S3_TEST_FOLDER_TOPIC_NAME + "/"));
    assertThat(iterator.next(), is(OTHER_S3_TEST_FOLDER_TOPIC_NAME + "/"));
  }

  @Test
  public void getFirstFile() throws IOException {
    String folder = "/year=1945/month=03/day=07/hour=22/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof HourlyPartitioner);
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
    String folder = "/year=1980/month=04/day=09/hour=04/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+1+0000000000.avro", avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof HourlyPartitioner);
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
        is(S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+1+0000000000.avro")
    );
  }

  @Test
  public void getSecondFileDifferentFolder() throws IOException {
    String folder = "/year=2011/month=11/day=02/hour=23/";
    String folder2 = "/year=2016/month=02/day=04/hour=19/";
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
    assertTrue(partitioner instanceof HourlyPartitioner);
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
    String folder = "/year=2001/month=10/day=02/hour=14/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );

    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof HourlyPartitioner);
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
}
