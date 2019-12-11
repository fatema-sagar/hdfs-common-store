/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.cloud.storage.source.format;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public class ByteArrayConverter implements Converter {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (schema != null && schema.type() != Schema.Type.BYTES) {
      throw new DataException(
          "Invalid schema type for ByteArrayConverter: " + schema.type().toString());
    }

    if (value != null && !(value instanceof byte[])) {
      throw new DataException(
          "ByteArrayConverter is not compatible with objects of type " + value.getClass());
    }

    return (byte[]) value;
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
  }
}

