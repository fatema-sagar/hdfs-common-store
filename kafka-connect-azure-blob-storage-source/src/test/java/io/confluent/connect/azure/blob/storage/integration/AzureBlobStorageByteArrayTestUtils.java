/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.azure.blob.storage.integration;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

public class AzureBlobStorageByteArrayTestUtils {

  private static final byte[] LINE_SEPARATOR = System.lineSeparator().getBytes();

  @SuppressWarnings("resource")
  public static Path writeByteArrayFile(int messages,  int startingIndex) throws IOException {
    Path file = Files.createTempFile("users-", ".bin");
    ByteArrayConverter converter = new ByteArrayConverter();

    for (int i = startingIndex; i < messages; i++) {
      byte[] messageBytes = converter.fromConnectData("", Schema.BYTES_SCHEMA, Base64.getEncoder()
          .encode(SerializationUtils.serialize(new User("John", "Smith", i))));
      Files.write(file, messageBytes, StandardOpenOption.APPEND);
      Files.write(file, LINE_SEPARATOR, StandardOpenOption.APPEND);
    }

    return file;
  }

  public static long bytesRecordSize(SourceRecord record) {
    return ((byte[]) record.value()).length + LINE_SEPARATOR.length;
  }
  
  //Purely for use in tests. Preferable to checking quality on json strings / objects
  public static class User implements Serializable {
   private String firstname;
   private String lastname;
   private Integer age;
   
   public User(String firstname, String lastname, Integer age) {
     this.firstname = firstname;
     this.lastname = lastname;
     this.age = age;
   }
   
  }

  
}
