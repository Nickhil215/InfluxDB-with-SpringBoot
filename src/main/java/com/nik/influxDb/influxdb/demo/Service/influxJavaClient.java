package com.nik.influxDb.influxdb.demo.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.nik.influxDb.influxdb.demo.Model.DataInfo;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class influxJavaClient {

  Logger LOGGER = Logger.getLogger(influxJavaClient.class.getName());

  InfluxDBClient influxDBClient;
  WriteApiBlocking writeApi;
  int i = 0;
  private char[] token = "sx3ZNuuivhpSMxIQVq8RBuFPmFucBeFC1hyiJ-YmOVtnETiQ_8SCFNjJ7LZ3G8L47z1IEKfEMpYjt5hthdXYwg==".toCharArray();
  private String org = "nikhil";
  private String bucket = "Nikhil";

  public influxJavaClient() {
    influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
    writeApi = influxDBClient.getWriteApiBlocking();
  }

//  @Scheduled(fixedRate = 10000  )
  public void insertDataIntoTable() {
    int age = (int) (Math.random() * 100) + 1;
    LOGGER.info("========= ::::::::::::  Age ::::::::  ======== " + age);
    Point point = Point.measurement("userGaian")
        .time(System.currentTimeMillis(), WritePrecision.MS)
        .addTag("name", "Guru")
        .addTag("Location", "Hyd")
        .addTag("Country", "India")
        .addField("Age",age);
    writeApi.writePoint(point);
  }

  public Point insertData(DataInfo data) {
    LOGGER.info("Data :: " + data);
    Point point = Point.measurement(data.getTableName())
        .time(System.currentTimeMillis(), WritePrecision.MS)
        .addTag("name", data.getName())
        .addTag("Location", data.getLocation())
        .addTag("Country", data.getCountry())
        .addField("DOB", Year.now().getValue() - data.getAge())
        .addField("Age", data.getAge());
    LOGGER.info("age :: " + data.getAge());
    writeApi.writePoint(point);
    return point;
  }

//  @Scheduled(fixedRate = 600)
  public void insertContinoues() {
    Point point = Point.measurement("Ages")
        .time(System.currentTimeMillis(), WritePrecision.MS)
        .addTag("name", "server1")
        .addField("insertionCount", i);
    writeApi.writePoint(point);
    i++;
  }


  // inside your InfluxJavaClient class
  public void deleteData(String measurementName, String name) {
    // We are deleting all points where tag 'name' equals to the provided name
    OffsetDateTime start = OffsetDateTime.parse("1970-01-01T00:00:00Z"); // start of Unix epoch
    OffsetDateTime stop = OffsetDateTime.parse(Instant.now().toString()); // current time
    String predicate = String.format("_measurement=\"%s\" AND name=\"%s\"", measurementName, name);

    influxDBClient.getDeleteApi().delete(start, stop, predicate, bucket, org);
    LOGGER.info(
        "Data deleted where measurement name is " + measurementName + " and name is " + name);
  }

  //  public List<FluxRecord> getData(String measurementName) {
//    String getQuery = String.format(
//        "from(bucket: \"%s\") |> range(start: -24h) |> filter(fn: (r) => r._measurement == \"%s\")",
//        bucket, measurementName);
//    LOGGER.info("getData Query is " + getQuery);
//
//    List<FluxTable> tables = influxDBClient.getQueryApi().query(getQuery, org);
//
//    List<FluxRecord> records = new ArrayList<>();
//    for (FluxTable table : tables) {
//      records.addAll(table.getRecords());
//    }
//    return records;
//  }
  public List<FluxRecord> getData(String bucket, String measurementName, String start, String end) {
    String getQuery = String.format(
        "from(bucket: \"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\")",
        bucket, start, end, measurementName);
    LOGGER.info("get data Query is " + getQuery);

    List<FluxTable> tables = influxDBClient.getQueryApi().query(getQuery, org);

    List<FluxRecord> records = new ArrayList<>();
    for (FluxTable table : tables) {
      records.addAll(table.getRecords());
    }
    return records;
  }

  public List<FluxRecord> getDataWithFilter(String bucket, String measurementName, String start,
      String end, Map<String, Object> filters) {
    StringBuilder filterBuilder = new StringBuilder();
    filterBuilder.append(String.format(
        "from(bucket: \"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\")",
        bucket, start, end, measurementName));
    for (Map.Entry<String, Object> filter : filters.entrySet()) {
      if(filter.getValue() instanceof Integer){
        filterBuilder.append(String.format(" |> filter(fn: (r) => r.%s == %d)", filter.getKey(),
            filter.getValue()));

      }else{
        filterBuilder.append(String.format(" |> filter(fn: (r) => r.%s == \"%s\")", filter.getKey(),
            filter.getValue()));
      }
    }

    String getQuery = filterBuilder.toString();
    LOGGER.info("getDataWithFilter Query is :: " + getQuery);
    List<FluxTable> tables = influxDBClient.getQueryApi().query(getQuery, org);
    List<FluxRecord> records = new ArrayList<>();
    for (FluxTable table : tables) {
      records.addAll(table.getRecords());
    }
    return records;
  }

  public void DeleteWithFilters(String bucket, String measurementName, String startTime, String end,
      Map<String, Object> filters) {
    OffsetDateTime start = OffsetDateTime.parse(startTime);
    OffsetDateTime stop = OffsetDateTime.parse(end);
    StringBuilder predicateBuilder = new StringBuilder();
    predicateBuilder.append(String.format("_measurement=\"%s\"", measurementName));
    for (Map.Entry<String, Object> filter : filters.entrySet()) {
      predicateBuilder.append(String.format(" AND %s=\"%s\"", filter.getKey(), filter.getValue()));
    }
    String predicate = predicateBuilder.toString();
    influxDBClient.getDeleteApi().delete(start, stop, predicate, bucket, org);
    LOGGER.info("Data deleted where measurement name is " + measurementName);

  }

  public FluxRecord getLatestData(String bucket, String measurementName, String start, String end, Map<String,Object>filters) {
    String getQuery = String.format(
        "from(bucket: \"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\") |> sort(columns: [\"_time\"], desc: true) |> limit(n:1)",
        bucket, start, end, measurementName);

    for (Map.Entry<String, Object> filter : filters.entrySet()) {
      getQuery+= (String.format(" |> filter(fn: (r) => r.%s == \"%s\")", filter.getKey(),
          filter.getValue()));
    }
    LOGGER.info("get data Query is " + getQuery);

    List<FluxTable> tables = influxDBClient.getQueryApi().query(getQuery, org);

    if (tables.isEmpty() || tables.get(0).getRecords().isEmpty()) {
      return null; // or throw an exception
    }

    return tables.get(0).getRecords().get(0);
  }

}