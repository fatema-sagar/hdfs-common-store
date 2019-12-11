/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.common;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The type Employee base class which houses all the common needs.
 */
public class EmployeeBase {

  protected static final JsonConverter jsonConverter = createJsonConverter();
  /**
   * List of names that can be used for testing purposes.
   */
  protected static final List<String> employeeNames = Arrays.asList("Lorenzo Domenico",
      "Ozell Perrine");

  /**
   * List of dobs that can be used for testing purposes.
   */
  protected static final List<java.util.Date> employeeDobs = Arrays.asList(
      new java.util.Date(2012,01,26, 00, 00, 00),
      new java.util.Date(1969,1,12, 00, 00, 00),
      new java.util.Date(1972,10,10, 00, 00, 00),
      new java.util.Date(2042,04,16, 00, 00, 00),
      new java.util.Date(1982,1,15, 00, 00, 00),
      new java.util.Date(1996,8,05, 00, 00, 00),
      new java.util.Date(2041,11,27, 00, 00, 00),
      new java.util.Date(2067,7,12, 00, 00, 00),
      new java.util.Date(1941,6,20, 00, 00, 00),
      new java.util.Date(2045,9,22, 00, 00, 00),
      new java.util.Date(2018,9,15, 00, 00, 00),
      new java.util.Date(2079,10,27, 00, 00, 00),
      new java.util.Date(2050,5,27, 00, 00, 00),
      new java.util.Date(1996,2,19, 00, 00, 00),
      new java.util.Date(1928,5,15, 00, 00, 00),
      new java.util.Date(2097,3,25, 00, 00, 00),
      new java.util.Date(2043,6,06, 00, 00, 00),
      new java.util.Date(2077,8,01, 00, 00, 00),
      new java.util.Date(1919,6,10, 00, 00, 00),
      new java.util.Date(1973,7,28, 00, 00, 00));

  /**
   * List of salaries that can be used for testing purposes.
   */
  protected static final List<Double> employeeSalaries = Arrays.asList(38224.81,
      57885.58, 75498.3, 99433.21, 17324.62, 49773.43, 66148.54, 37480.03, 84990.03, 75244.41,
      57817.51, 42679.51, 41963.29, 26592.72, 80379.54, 73246.08, 36850.46, 29621.19, 16016.28,
      40115.53);

  /**
   * List of contact numbers that can be used for testing purposes.
   */
  protected static final List<String> employeePhoneNumbers = Arrays.asList("+00 9667246331",
      "+00 9052731880", "+00 7621323706", "+00 7208892360", "+00 9931345587", "+00 7336062335",
      "+00 7025389560", "+00 7361568676", "+00 7360348758", "+00 8940110917", "+00 9518549952",
      "+00 9114341593", "+00 7586513803", "+00 8999964259", "+00 8604115227", "+00 8396248573",
      "+00 8265023398", "+00 9692524690", "+00 9367820798", "+00 9987411473");


  /**
   * The EMPLOYEE schema.
   */
  protected static final Schema EMPLOYEE_SCHEMA_V1 = new SchemaBuilder(Schema.Type.STRUCT).name(
      "employee")
      .field("measurement", Schema.STRING_SCHEMA)
      .field("tags", Schema.STRING_SCHEMA)
      .field("time", Schema.INT64_SCHEMA)
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      // TODO: identify why Avro doesnt support date formats.
      // Based on Rahul's code comments, AVRO doesnt support complex types. But looks like it doesnt
      // Work for date and time as well.
      // .field("dob", Timestamp.builder())
      .field("contactNumber", SchemaBuilder.STRING_SCHEMA)
      .field("salary", Schema.FLOAT64_SCHEMA)
      .field("active", Schema.BOOLEAN_SCHEMA)
      .build();

  /**
   * Create json converter json converter.
   *
   * @return the json converter
   */
  static JsonConverter createJsonConverter() {
    Map<String, String> config = new HashMap<>();
    config.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
    config.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());

    JsonConverter converter = new JsonConverter();
    converter.configure(config);
    return converter;
  }

  /**
   * Get a random employee name.
   *
   * @return the string
   */
  protected String getName(){
    return employeeNames.get(getRandomIntegerBetweenRange(0, 1));
  }

  /**
   * Get a random employee salary.
   *
   * @return the double
   */
  protected Double getSalary(){
    return employeeSalaries.get(getRandomInteger());
  }

  /**
   * Get a random employee contact number.
   *
   * @return the string
   */
  protected String getPhoneNumber(){
    return employeePhoneNumbers.get(getRandomInteger());
  }

  /**
   * Get a random employee DOB.
   *
   * @return the java . util . date
   */
  protected java.util.Date getDob(){
    return employeeDobs.get(getRandomInteger());
  }


  /**
   * Get a random integer between 0 and 19.
   * Our hardcoded lists contain 20 sets of mock data each and hence why between 0 and 19.
   *
   * @return the int
   */
  protected static int getRandomInteger(){
    int min = 0, max =19;
    return  (int)(Math.random()*((max-min)+1))+min;
  }

  /**
   * Get random integer between range int.
   *
   * @param min the min
   * @param max the max
   * @return the int
   */
  protected static int getRandomIntegerBetweenRange(int min, int max){
    return  (int)(Math.random()*((max-min)+1))+min;
  }
}
