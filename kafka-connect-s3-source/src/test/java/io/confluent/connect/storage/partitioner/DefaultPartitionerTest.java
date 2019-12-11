/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.storage.partitioner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import io.confluent.connect.s3.source.TestWithMockedS3;

import static io.confluent.connect.s3.source.S3AvroTestUtils.writeAvroFile;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DefaultPartitionerTest extends TestWithMockedS3 {

  @Before
  public void setup() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }


  @Test
  public void get1PartitionHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertThat(partitioner.getPartitions().size(), is(1));
    assertThat(
        partitioner.getPartitions().iterator().next(),
        is(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/")
    );
  }

  @Test
  public void get2PartitionsHappyPath() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=1/test-topic+1+0000000000.avro",
        avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertThat(partitioner.getPartitions().size(), is(2));
    assertTrue(partitioner.getPartitions().contains(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/"));
    assertTrue(partitioner.getPartitions().contains(S3_TEST_FOLDER_TOPIC_NAME + "/partition=1/"));
  }

  @Test
  public void getFirstFile() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), null);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro")
    );
  }

  @Test
  public void getSecondFileSameFolder() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000010000.avro",
        avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), null);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro")
    );
    String secondObject = partitioner
        .getNextObjectName(
            partitioner.getPartitions().iterator().next(),
            firstObject
        );
    assertNotNull(secondObject);
    assertThat(
        secondObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000010000.avro")
    );
  }

  @Test
  public void getSecondFileDifferentFolder() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );
    Path avroFile2 = writeAvroFile(1);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=1/test-topic+1+0000000000.avro",
        avroFile2.toFile()
    );
    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    String firstObject = partitioner
        .getNextObjectName(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/", null);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro")
    );
    String secondObject = partitioner
        .getNextObjectName(
            S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/",
            firstObject
        );
    assertNotNull(secondObject);
  }

  @Test
  public void getSecondFileWhichDoesntExist() throws IOException {
    Path avroFile = writeAvroFile(1);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    s3.putObject(
        S3_TEST_BUCKET_NAME,
        S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro",
        avroFile.toFile()
    );

    Partitioner partitioner = connectorConfig.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    String firstObject = partitioner
        .getNextObjectName(partitioner.getPartitions().iterator().next(), null);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is(S3_TEST_FOLDER_TOPIC_NAME + "/partition=0/test-topic+0+0000000000.avro")
    );
    String secondObject = partitioner
        .getNextObjectName(
            partitioner.getPartitions().iterator().next(),
            firstObject
        );
    assertNotNull(secondObject);
  }
}
