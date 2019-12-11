/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.connect.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Struct;

public class Employee extends EmployeeBase{

  private final Struct struct;

  /**
   * Instantiates a new Employee.
   *
   * @param id the id
   */
  public Employee(int id) {
    struct = new Struct(EMPLOYEE_SCHEMA_V1)
        .put("measurement", "employee")
        .put("tags", "{\"id\":\""+id+"\"}")
        .put("time", System.currentTimeMillis())
        .put("id",id)
        .put("name",getName())
        // For DOB see TODO in EmployeeBase schema for dob.
        // .put("dob", getDob())
        .put("contactNumber", getPhoneNumber())
        .put("salary", getSalary())
        .put("active",
            Boolean.valueOf(Integer.toString(getRandomIntegerBetweenRange(0, 1))))
    ;
  }

  /**
   * Generate a list of employees.
   *
   * @param count the count
   * @return the list
   * @throws InterruptedException the interrupted exception
   */
  public static List<Employee> generateEmployees(int count) throws InterruptedException {
    List<Employee> employees = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      employees.add(new Employee(i));
    }
    return employees;
  }

  /**
   * As json with schema string.
   *
   * @return the string
   */
  public String asJsonWithSchema() {
    byte[] employeeAsBytes = jsonConverter.fromConnectData("topic", this.struct.schema(), this.struct);
    return new String(employeeAsBytes);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((struct == null) ? 0 : struct.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;

    Employee other = (Employee) obj;
    if (struct == null) {
      if (other.struct != null)
        return false;
    } else if (!struct.equals(other.struct))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Employee {struct=" + struct + "}";
  }
}
