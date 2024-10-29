package com.example.customer;



import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.confluent.ksql.api.client.*;

@RestController
@RequestMapping("api/v1/customer3l/")
// @SpringBootApplication
public class KafkaTest {

    // @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // @Autowired
    private static ClientOptions ksqlClientOptions;

    private static Properties props = new Properties();
    private static Map<String, Object> properties;

    private static String topicName = "CUSTOMER_3L"; // CUSTOMER_3L_TEMP
    private static String streamName = "CUSTOMER_3L_STREAM"; // CUSTOMER_3L_TEMP_STREAM
    private static KafkaProducer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;
    private static String kafkaString = "{\"accountno\":\"0\",\"totime\":\"00:00\",\"aggrid\":\"CUST_1212000000021233\",\"fromdate\":\"2024-09-02\",\"todate\":\"5054-04-12\",\"fromtime\":\"00:00\",\"timeflag\":\"N\"}";

    public static void main(String[] args) throws Exception {

        KafkaTest kafkaTest = new KafkaTest();
        kafkaTest.init();
        System.out.println(new Date());
        kafkaTest.sendToKafka(kafkaTest, kafkaString);
//         List<String> records = kafkaTest.getRecords();
//         System.out.println("Record Size is :" + records.size());

    }

    public void sendToKafka(KafkaTest kafkaTest, String kafkaString) {
        try {
            for (int i = 0; i <= 500000; i++) {
                kafkaTest.sendToKafka(kafkaString);
                Thread.sleep(5);
            }
        } catch (Exception ex) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                //Do Nothing
            }
            kafkaTest.sendToKafka(kafkaTest, kafkaString);
        }
    }

    public void sendToKafka(String kafkaString) {

        KafkaTemplate<String, String> kafkaTemplate1 = new KafkaTemplate<>(producerFactory());

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate1.send(topicName, kafkaString);
        AtomicReference<String> exception = new AtomicReference<>("");
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("sendPaymentSuccess with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                exception.set("Unable to send message due to ~~~" + ex.getMessage() + "~~~");
                System.out.println(exception.get());
            }
        });
        if (exception.get().length() > 0) {
            System.out.println(exception.toString());
        }
    }

    public List<String> getRecords() {
        // HashMap<Integer, String> records = new HashMap<Integer, String>();
        List<String> records = new ArrayList<>();
        try {
            String pmtStatusQuery = "SELECT rowtime, * FROM " + streamName + ";";// limit 10;";

            Client client = Client.create(ksqlClientOptions);
            List<Row> rowsStream = null;
            try {
                System.out.println(client.listStreams());
                System.out.println(client.listTopics());
                System.out.println("Start to read stream");
                BatchedQueryResult result = client.executeQuery(pmtStatusQuery);
                if (result != null) {
                    rowsStream = result.get();// timeout, TimeUnit.SECONDS);
                }
                // rowsStream = client.executeQuery(pmtStatusQuery, properties).get();//30,
                // TimeUnit.SECONDS);
                client.close();
                System.out.println("rowStream Size :" + rowsStream.size());

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            rowsStream.stream().forEach(row -> records.add(row.toString()));// .getString(0)));

            // for (String record : records) {
            //     System.out.println("Record is:" + record);
            // }

            // if (records != null && !records.isEmpty()) {
            // for (Map.Entry<Integer, String> entry : records.entrySet()) {
            // System.out.println("Key:" + entry.getKey() + " Value:" + entry.getValue());
            // }
            // }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return records;
    }

    private void init() {
        properties = Collections.singletonMap("auto.offset.reset", "earliest");

        ksqlClientOptions = ClientOptions.create()
                .setHost("localhost")
                .setPort(8088)
                .setExecuteQueryMaxResultRows(10000000);

    }

    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @GetMapping("/get-records")
    public List<String> getRecordsURL() {
        try {
            KafkaTest kafkaTest = new KafkaTest();
            kafkaTest.init();
            List<String> records = kafkaTest.getRecords();
            System.out.println("Record Size is :" + records.size());
            if(records.size()>200000){
                List<String> value=new ArrayList<>();
                value.add("Dear User, Statement Data extracted is more than accepted range. Please lower your range in terms of end date and share request again.");
//                value.add("Please Narrow Your serach cretiera");
                return  value;


            }
            return records;
        } catch (Exception ex) {
            ArrayList<String> arrayList = new ArrayList<String>();
            arrayList.add(ex.getMessage());
            return arrayList;
        }
        // return null;
    }

    /*
     * CREATE STREAM IF NOT EXISTS CUSTOMER_3L_STREAM (accountno STRING, totime
     * STRING, aggrid STRING, fromdate STRING, todate STRING, fromtime STRING,
     * timeflag STRING) WITH (kafka_topic='CUSTOMER_3L', partitions=1,
     * value_format='JSON');
     *
     * {
     * "accountno": "0",
     * "totime": "00:00",
     * "aggrid": "CUST_1212000000021233",
     * "fromdate": "2024-09-02",
     * "todate": "5054-04-12",
     * "fromtime": "00:00",
     * "timeflag": "N"
     * }
     */

}

