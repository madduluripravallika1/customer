package com.example.customer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CustomerApplication {

	public static void main(String[] args) {
		SpringApplication.run(CustomerApplication.class, args);
	}

	//./kafka-topics --create --bootstrap-server 127.0.0.1:9091 --replication-factor 1 --partitions 1 --topic CUSTOMER_3L --config min.insync.replicas=1
	//CREATE STREAM IF NOT EXISTS CUSTOMER_3L_STREAM (accountno STRING, totime STRING, aggrid STRING, fromdate STRING, todate STRING, fromtime STRING, timeflag STRING) WITH (kafka_topic='CUSTOMER_3L', partitions=1, value_format='JSON');

}
