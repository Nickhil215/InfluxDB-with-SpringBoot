package com.nik.influxDb.influxdb.demo.Controller;

import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.nik.influxDb.influxdb.demo.Model.DataInfo;
import com.nik.influxDb.influxdb.demo.Service.influxJavaClient;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class influxdbJavaClientCntrl {

  private final influxJavaClient client;

  @GetMapping("/insertData")
  public void insertData() {
//    client.insertDataIntoTable("userGaian");
  }

//  @GetMapping("/getAlldata/{tableName}")
//  public List<FluxRecord> getData(@PathVariable String tableName) {
//    return client.getData(tableName);
//  }

  @PostMapping("/insert")
  public Point insertData(@RequestBody DataInfo data) {
    return client.insertData(data);
  }

  @GetMapping("/getData/{bucket}/{measurementName}")
  public List<FluxRecord> getData(@PathVariable String bucket,
      @PathVariable String measurementName, @RequestParam(defaultValue = "-24h") String start,
      @RequestParam(defaultValue = "#{T(java.time.Instant).now().toString()}") String end) {
    return client.getData(bucket, measurementName, start, end);
  }

  @PostMapping("/getData/{bucket}/{measurementName}")
  public List<FluxRecord> getDataWithFilter(@RequestBody() Map<String, Object> filters,
      @PathVariable String bucket, @PathVariable String measurementName,
      @RequestParam(defaultValue = "-24h") String start,
      @RequestParam(defaultValue = "#{T(java.time.Instant).now().toString()}") String end) {
    return client.getDataWithFilter(bucket, measurementName, start, end, filters);
  }


  @DeleteMapping("/delete/{table}/{name}")
  public void deleteData(@PathVariable String table, @PathVariable String name) {
    client.deleteData(table, name);
  }

  @PostMapping("/deleteData/{bucket}/{measurementName}")
  public void deleteDataWithFilter(@RequestBody() Map<String, Object> filters,
      @PathVariable String bucket, @PathVariable String measurementName,
      @RequestParam(defaultValue = "1970-01-01T00:00:00Z") String start,
      @RequestParam(defaultValue = "#{T(java.time.Instant).now().toString()}") String end) {
    client.DeleteWithFilters(bucket, measurementName, start, end, filters);
  }




//
//  @GetMapping("/insertContinoues")
//  public void insertContinoues() {
//    client.insertContinoues();
//  }



  @PostMapping("/getLatestData/{bucket}/{measurementName}")
  public FluxRecord getLatestData(@PathVariable String bucket,
      @PathVariable String measurementName, @RequestParam(defaultValue = "-24h") String start,
      @RequestParam(defaultValue = "#{T(java.time.Instant).now().toString()}") String end,@RequestBody() Map<String, Object> filters) {

    return client.getLatestData(bucket, measurementName, start, end, filters);
  }



}
