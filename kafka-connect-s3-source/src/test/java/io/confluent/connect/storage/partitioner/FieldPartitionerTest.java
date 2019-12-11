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

import io.confluent.connect.s3.source.S3SourceConnectorConfig;
import io.confluent.connect.s3.source.TestWithMockedS3;

import static io.confluent.connect.s3.source.S3AvroTestUtils.writeAvroFile;
import static io.confluent.connect.s3.source.S3SourceConnectorConfig.PARTITIONER_CLASS_CONFIG;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FieldPartitionerTest extends TestWithMockedS3 {

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
    props.put(PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getCanonicalName());
    props.put(S3SourceConnectorConfig.PARTITION_FIELD_NAME_CONFIG, "firstName,surname,age");
    return props;
  }

  @Test
  public void get1PartitionHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/John/Smith/21/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof FieldPartitioner);
    assertThat(partitioner.getPartitions().size(), is(1));
    assertThat(
        partitioner.getPartitions().iterator().next(),
        is(S3_TEST_FOLDER_TOPIC_NAME + "/John/Smith/21/")
    );
  }

  @Test
  public void get2PartitionsHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/John/Smith/21/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/Jane/Doe/21/test-topic+0+0000000000.avro",
        avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof FieldPartitioner);
    assertThat(partitioner.getPartitions().size(), is(2));
    final Iterator<String> iterator = partitioner.getPartitions().iterator();
    assertThat(
        iterator.next(),
        is(S3_TEST_FOLDER_TOPIC_NAME + "/Jane/Doe/21/")
    );
    assertThat(
        iterator.next(),
        is(S3_TEST_FOLDER_TOPIC_NAME + "/John/Smith/21/")
    );
  }

  @Test
  public void getFirstFile() throws IOException {
    String folder = "/Matt/Smith/21/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof FieldPartitioner);
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
    String folder = "/kevin/Heart/21/";
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
    assertTrue(partitioner instanceof FieldPartitioner);
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
    String folder = "/Amanda/Man/21/";
    String folder2 = "/Joe/White/24/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder2 + "test-topic+0+0000000000.avro", avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof FieldPartitioner);
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
    String folder = "/Kevin/Smith/21/";
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + folder + "test-topic+0+0000000000.avro", avroFile.toFile()
    );

    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof FieldPartitioner);
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
