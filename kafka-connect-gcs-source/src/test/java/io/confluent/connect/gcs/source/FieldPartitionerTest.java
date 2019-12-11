/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import static io.confluent.connect.cloud.storage.source.CloudStorageSourceConnectorCommonConfig.STORE_URL_CONFIG;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.storage.Storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;

public class FieldPartitionerTest extends GcsTestUtils {

  private Map<String, String> settings;
  private GcsSourceConnectorConfig config;
  private GcsSourceStorage storage;
  private Storage gcsStorageObject;

  @Before
  public void before() throws IOException {
    this.settings = getCommonConfig();
    this.settings.put("format.class", AVRO_FORMAT_CLASS);
    this.settings.put("folders", GCS_TEST_FOLDER_TOPIC_NAME);
    this.settings.put("partitioner.class", FIELD_PARTITIONER);
    this.settings.put("partition.field.name", "firstname,lastname");

    this.config = new GcsSourceConnectorConfig(settings);
    this.gcsStorageObject = uploadAvroDataToMockGcsBucket( "firstname=Virat/lastname=Kohli",
        "1");
    this.storage = new GcsSourceStorage(config.getString(STORE_URL_CONFIG), BUCKECT_NAME, this.gcsStorageObject);
  }

  @After
  public void after(){

  }

  @Test
  public void testDailyPartitionConfig(){
    Partitioner partitioner = config.getPartitioner(storage);
    assertTrue(partitioner instanceof FieldPartitioner);
    this.settings.remove("partitioner.class");
    this.config = new GcsSourceConnectorConfig(settings);

    partitioner = config.getPartitioner(storage);
    assertTrue(partitioner instanceof DefaultPartitioner);
  }

  @Test
  public void testPartitionerNextObject() throws IOException {
    Partitioner partitioner = config.getPartitioner(storage);
    assertThat(partitioner.getPartitions().size(), is(1));
    String firstObject =
        partitioner.getNextObjectName(partitioner.getPartitions().iterator().next(), null);
    assertNotNull(firstObject);
    assertThat(firstObject,
        is("topics/gcs_topic/firstname=Virat/lastname=Kohli/gcs_topic+0+0000000001.avro"));

    addNewAvroRecord(this.gcsStorageObject, "firstname=Kohli/lastname=Virat", 15, "1");
    assertThat(partitioner.getPartitions().size(), is(2));

    final Iterator<String> iterator = partitioner.getPartitions().iterator();

    assertThat(
        iterator.next(),
        is("topics/gcs_topic/firstname=Kohli/lastname=Virat/")
    );

    assertThat(
        iterator.next(),
        is("topics/gcs_topic/firstname=Virat/lastname=Kohli/")
    );
  }
}
