/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.STORE_URL_CONFIG;

import com.google.cloud.storage.Storage;

import org.junit.After;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;

public class DefaultPartitionerTest extends GcsTestUtils {

  private Map<String, String> settings;
  private GcsSourceConnectorConfig config;
  private GcsSourceStorage storage;
  private Storage gcsStorageObject;

  @Before
  public void before() throws IOException {
    this.settings = getCommonConfig();
    this.settings.put("format.class", AVRO_FORMAT_CLASS);
    this.settings.put("folders", GCS_TEST_FOLDER_TOPIC_NAME);
    this.config = new GcsSourceConnectorConfig(settings);
    this.gcsStorageObject = uploadAvroDataToMockGcsBucket("partition=0", "1");
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, this.gcsStorageObject);
  }

  @After
  public void after(){

  }

  @Test
  public void testDefaultPartitionConfig(){
    Partitioner partitioner = config.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);

    this.settings.remove("partitioner.class");
    this.config = new GcsSourceConnectorConfig(settings);

    partitioner = config.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
  }

  @Test
  public void testPartitionerNextObject() throws IOException {
    Partitioner partitioner = config.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
    assertThat(partitioner.getPartitions().size(), is(1));

    addNewAvroRecord(this.gcsStorageObject, "partition=1", 15, "1");

    assertThat(partitioner.getPartitions().size(), is(2));

    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), null);
    assertNotNull(firstObject);
    assertThat(
        firstObject,
        is("topics/gcs_topic/partition=0/gcs_topic+0+0000000001.avro")
    );

    addNewAvroRecord(this.gcsStorageObject, "partition=1", 15, "2");
    addNewAvroRecord(this.gcsStorageObject, "partition=1", 15, "3");

    String secondObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), firstObject);
    assertNotNull(secondObject);
  }

}
