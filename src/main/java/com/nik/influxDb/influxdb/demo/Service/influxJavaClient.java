/*
 * Copyright (c) 2024.
 *    Author :: Nikhil Vanamala.
 *    All rights reserved To NIKHIL VANAMALA.
 * .
 */

package com.nik.influxDb.influxdb.demo.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.nik.influxDb.influxdb.demo.Model.DataInfo;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;


@Service
public class influxJavaClient {

  private final char[] token = "sx3ZNuuivhpSMxIQVq8RBuFPmFucBeFC1hyiJ-YmOVtnETiQ_8SCFNjJ7LZ3G8L47z1IEKfEMpYjt5hthdXYwg==".toCharArray();
  private final String org = "nikhil";
  private final String bucket = "Nikhil";
  Logger LOGGER = Logger.getLogger(influxJavaClient.class.getName());
  InfluxDBClient influxDBClient;
  WriteApiBlocking writeApi;
  int i = 0;

  public influxJavaClient() {
    influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
    writeApi = influxDBClient.getWriteApiBlocking();
  }

  //  @Scheduled(fixedRate = 10000)
  public void insertDataIntoTable() {
    int age = (int) (Math.random() * 100) + 1;
    LOGGER.info("========= ::::::::::::  Age ::::::::  ======== " + age);
    Point point = Point.measurement("userGaian")
        .time(System.currentTimeMillis(), WritePrecision.MS)
        .addTag("name", "Guru")
        .addTag("Location", "Hyd")
        .addTag("Country", "India")
        .addField("Age", age);
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

    return getFluxRecords(getQuery);
  }

  @NotNull
  private List<FluxRecord> getFluxRecords(String getQuery) {
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
      if (filter.getValue() instanceof Integer) {
        filterBuilder.append(String.format(" |> filter(fn: (r) => r.%s == %d)", filter.getKey(),
            (int) filter.getValue()));

      } else {
        filterBuilder.append(String.format(" |> filter(fn: (r) => r.%s == \"%s\")", filter.getKey(),
            filter.getValue()));
      }
    }

    String getQuery = filterBuilder.toString();
    LOGGER.info("getDataWithFilter Query is :: " + getQuery);
    return getFluxRecords(getQuery);
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

  public FluxRecord getLatestData(String bucket, String measurementName, String start, String end,
      Map<String, Object> filters) {
    StringBuilder getQueryBuilder = new StringBuilder();
    getQueryBuilder.append(String.format(
        "from(bucket: \"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\") |> sort(columns: [\"_time\"], desc: true) |> limit(n:1)",
        bucket, start, end, measurementName));

    for (Map.Entry<String, Object> filter : filters.entrySet()) {
      getQueryBuilder.append(String.format(" |> filter(fn: (r) => r.%s == \"%s\")", filter.getKey(),
          filter.getValue()));
    }
    LOGGER.info("get data Query is " + getQueryBuilder);

    List<FluxTable> tables = influxDBClient.getQueryApi().query(getQueryBuilder.toString(), org);

    if (tables.isEmpty() || tables.get(0).getRecords().isEmpty()) {
      return null; // or throw an exception
    }

    return tables.get(0).getRecords().get(0);
  }

  //  csv to influxdb

  public void insertCSVDataFromUrl() throws IOException {
    String csvUrl = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(csvUrl);
      try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
        String csvData = EntityUtils.toString(response.getEntity());
        try (StringReader in = new StringReader(csvData)) {
          Iterable<CSVRecord> records = CSVFormat.DEFAULT
              .withHeader("TableName", "Name", "Location", "Country", "Age")
              .withFirstRecordAsHeader()
              .parse(in);
          LOGGER.info("IN Records :: " + in);
          LOGGER.info("Records :: " + records);
          convertCSVRecordToList(records);
        }
      }
    }
  }


  //  @Scheduled(fixedRate = 60000)
  public List<Map<String, Object>> insertCSVDataFromUrl1() throws IOException {
    String csvUrl = "https://drive.google.com/uc?id=1x2IdSNcHGLmot9i1h90gwMJr5lULC2QV&export=download";
    List<Map<String, Object>> data = new ArrayList<>();
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(csvUrl);
      try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
        String csvData = EntityUtils.toString(response.getEntity());
        try (StringReader in = new StringReader(csvData)) {
          Iterable<CSVRecord> records = CSVFormat.DEFAULT
              .withFirstRecordAsHeader()
              .parse(in);
          for (CSVRecord record : records) {
            Map<String, Object> row = new TreeMap<>();
            for (String header : record.toMap().keySet()) {
              row.put(header, record.get(header));
            }
            data.add(row);
          }
        }
      }
    }
    //    LOGGER.info("Data :: " + data);
    insertDataCSVMap(data);
    return data;
  }

  public List<Map<String, Object>> convertCSVRecordToList(Iterable<CSVRecord> records) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (CSVRecord record : records) {
      Map<String, Object> map = new HashMap<>();
      for (int i = 0; i < record.size(); i++) {
        map.put(record.getParser().getHeaderNames().get(i), record.get(i));
      }
      list.add(map);
    }
//    LOGGER.info("++++List? :: " + list);
    insertDataCSVMap(list);
    return list;
  }


  public void insertDataCSV(Iterable<CSVRecord> records) {
    List<Point> points = new ArrayList<>();
    for (CSVRecord record : records) {
      LOGGER.info("Record :: " + record.size());
      LOGGER.info("Record :: " + record);
      Point point = Point.measurement("CSV DATA")
          .time(System.currentTimeMillis(), WritePrecision.MS)
          .addTag("Month", record.get(0))
          .addField("1958_yr", record.get(1))
          .addField("1959_yr", record.get(2))
          .addField("1960_yr", record.get(3));
      points.add(point);
    }
    writeApi.writePoints(points);
  }


  public void insertDataCSVMap(List<Map<String, Object>> records) {
    List<Point> points = new ArrayList<>();
    for (Map<String, Object> record1 : records) {
      Point point = Point.measurement("CSV__DATA")
//          .addField("Age", 25)
          .time(System.currentTimeMillis(), WritePrecision.MS);

      for (Entry<String, Object> record : record1.entrySet()) {
        LOGGER.info("Record :: KEY ==> " + record.getKey() + "  :: VALUE ==> " + record.getValue());
        if (record.getValue() instanceof String) {
          point.addTag(record.getKey(), record.getValue().toString());
          if (record.getKey().equalsIgnoreCase("Country")) {
            point.addField(record.getKey(), record.getValue().toString());
          }
        } else {
          point.addField(record.getKey(), (int) record.getValue());
        }
      }
      points.add(point);
    }
    writeApi.writePoints(points);
  }

}