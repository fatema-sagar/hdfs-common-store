/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source;

import org.junit.Test;

import io.confluent.connect.cloud.storage.source.util.StorageObjectKeyParts;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StorageObjectKeyPartsTest {

  private String fileNameRegexPattern = "(.+)\\+(\\d+)\\+.+$";

  @Test
  public void shouldExtractKeyPartsBasedOnObjectKey() {
    String dirDelim = "/";
    String objectKey = "topics/test-topic/partition=0/test-topic+0+000000000.bin";

    Optional<StorageObjectKeyParts> maybekeyParts = StorageObjectKeyParts
        .fromKey(dirDelim, objectKey, fileNameRegexPattern);
    assertTrue(maybekeyParts.isPresent());

    StorageObjectKeyParts keyParts = maybekeyParts.get();
    assertThat(keyParts.getPartition(), is(0));
    assertThat(keyParts.getTopic(), is("test-topic"));
  }

  @Test
  public void shouldReturnEmptyOptionalForInvalidKey() {
    String dirDelim = "/";
    String objectKey = "invalid.json";

    Optional<StorageObjectKeyParts> maybekeyParts = StorageObjectKeyParts
        .fromKey(dirDelim, objectKey, fileNameRegexPattern);
    assertFalse(maybekeyParts.isPresent());
  }

  @Test
  public void shouldExtractKeyPartsFromKeyNameRegardlessOfPath() {
    String dirDelim = "/";
    String objectKey = "something-else/my-topic+9+000000010.avro";

    Optional<StorageObjectKeyParts> maybekeyParts = StorageObjectKeyParts
        .fromKey(dirDelim, objectKey, fileNameRegexPattern);
    assertTrue(maybekeyParts.isPresent());

    StorageObjectKeyParts keyParts = maybekeyParts.get();
    assertThat(keyParts.getPartition(), is(9));
    assertThat(keyParts.getTopic(), is("my-topic"));
  }

  @Test
  public void shouldExtractKeyPartsBasedOnObjectKeyForADifferentDelimiter() {
    String dirDelim = ";";
    String objectKey = "topics;test-topic;partition=0;test-topic+0+000000000.bin";

    Optional<StorageObjectKeyParts> maybekeyParts = StorageObjectKeyParts
        .fromKey(dirDelim, objectKey, fileNameRegexPattern);
    assertTrue(maybekeyParts.isPresent());

    StorageObjectKeyParts keyParts = maybekeyParts.get();
    assertThat(keyParts.getPartition(), is(0));
    assertThat(keyParts.getTopic(), is("test-topic"));
  }
}
