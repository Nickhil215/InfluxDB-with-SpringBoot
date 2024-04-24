package com.nik.influxDb.influxdb.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class InfluxdbDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(InfluxdbDemoApplication.class, args);
	}

}
