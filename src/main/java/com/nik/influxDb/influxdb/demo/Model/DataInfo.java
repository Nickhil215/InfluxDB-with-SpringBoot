package com.nik.influxDb.influxdb.demo.Model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataInfo {
  private String dbName;
  private String tableName;
  private String name;
  private String location;
  private String country;
  private int age;

  // getters and setters
}