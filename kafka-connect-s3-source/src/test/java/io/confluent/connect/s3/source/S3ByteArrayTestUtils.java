/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import io.confluent.connect.cloud.storage.source.StorageObject;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;


public class S3ByteArrayTestUtils {

  private static final byte[] LINE_SEPARATOR = System.lineSeparator().getBytes();

  public static S3TestUtils.User fromByteArray(SourceRecord record) {
    byte[] base64Decoded = Base64.getDecoder().decode((byte[]) record.value());

    return SerializationUtils.deserialize(base64Decoded);
  }

  public static StorageObject createByteArrayS3Object(int messages) throws IOException {
    Path file = writeByteArrayFile(messages, 0);

    S3Object object = new S3Object();
    object.setObjectContent(Files.newInputStream(file));
    object.setKey("topics/test-topic/partition=0/test-topic+0+0000000000.bin");
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(file.toFile().length());
    object.setObjectMetadata(objectMetadata);
    return new S3StorageObject(object);
  }

  @SuppressWarnings("resource")
  public static Path writeByteArrayFile(int messages,  int startingIndex) throws IOException {
    Path file = Files.createTempFile("users-", ".bin");
    ByteArrayConverter converter = new ByteArrayConverter();

    for (int i = startingIndex; i < messages; i++) {
      byte[] messageBytes = converter.fromConnectData("", Schema.BYTES_SCHEMA, Base64.getEncoder()
          .encode(SerializationUtils.serialize(new S3TestUtils.User("John", "Smith", i))));
      Files.write(file, messageBytes, StandardOpenOption.APPEND);
      Files.write(file, LINE_SEPARATOR, StandardOpenOption.APPEND);
    }

    return file;
  }

  public static long bytesRecordSize(SourceRecord record) {
    return ((byte[]) record.value()).length + LINE_SEPARATOR.length;
  }
}
