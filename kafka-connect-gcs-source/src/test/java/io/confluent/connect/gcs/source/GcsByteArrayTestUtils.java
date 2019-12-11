/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.gcs.source;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Objects;

import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class GcsByteArrayTestUtils {
  
  private static final byte[] LINE_SEPARATOR = System.lineSeparator().getBytes();

  public static Path writeByteArrayFile(int messages, int startingIndex) throws IOException {
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

  public static GcsUserObjectTestUtils.User fromByteArray(SourceRecord record) {
    byte[] base64Decoded = Base64.getDecoder().decode((byte[]) record.value());

    return SerializationUtils.deserialize(base64Decoded);
  }

  public static long bytesRecordSize(SourceRecord record) {
    return ((byte[]) record.value()).length + LINE_SEPARATOR.length;
  }
  
 //Purely for use in tests. Preferable to checking quality on json strings / objects
 static class User implements Serializable {
   private String firstname;
   private String lastname;
   private Integer age;

   public User() {
   }

   public User(GenericData.Record record) {
     this.firstname = String.valueOf(record.get("firstname"));
     this.lastname = String.valueOf(record.get("lastname"));
     this.age = (Integer) record.get("age");
   }

   public User(Struct struct) {
     this.firstname = struct.getString("firstname");
     this.lastname = struct.getString("lastname");
     this.age = struct.getInt32("age");
   }

   public User(String firstname, String lastname, Integer age) {
     this.firstname = firstname;
     this.lastname = lastname;
     this.age = age;
   }

   public String getFirstname() {
     return firstname;
   }

   public void setFirstname(String firstname) {
     this.firstname = firstname;
   }

   public String getLastname() {
     return lastname;
   }

   public void setLastname(String lastname) {
     this.lastname = lastname;
   }

   public Integer getAge() {
     return age;
   }

   public void setAge(Integer age) {
     this.age = age;
   }

   @Override
   public boolean equals(Object o) {
     if (this == o) {
       return true;
     }
     if (o == null || getClass() != o.getClass()) {
       return false;
     }
     User user = (User) o;
     return Objects.equals(firstname, user.firstname) && Objects.equals(lastname, user.lastname)
         && Objects.equals(age, user.age);
   }

   @Override
   public int hashCode() {
     return Objects.hash(firstname, lastname, age);
   }

   @Override
   public String toString() {
     return "User{" + "firstname='" + firstname + '\'' + ", lastname='" + lastname + '\''
         + ", age=" + age + '}';
   }
 }

}
