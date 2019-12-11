/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.s3.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class S3TestUtils {
  public static User fromAvro(SourceRecord record) throws IOException {
    String json = record.value().toString();
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(json, User.class);
  }

  // Purely for use in tests. Preferable to checking quality on json strings / objects
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
    
    public User(Map<String, Object> values) {
      this.firstname = (String) values.getOrDefault("firstname", "");
      this.lastname = (String) values.getOrDefault("lastname", "");
      this.age = ((Long) values.getOrDefault("age", 0L)).intValue();
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
