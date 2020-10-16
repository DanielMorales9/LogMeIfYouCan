package com.logmeifyoucan.generator;

import com.logmeifyoucan.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaStreamDataGenerator implements Runnable {


    public static void main(String[] args) {
        KafkaStreamDataGenerator gen = new KafkaStreamDataGenerator();
        gen.run();
    }

    public void run() {

        //Setup Kafka Client
        Properties kafkaProps = setupKafkaClientProperties();

        Producer<String,String> myProducer = new KafkaProducer<>(kafkaProps);

        BufferedReader reader;
        Long previousUnixTime = null;
        int speedFactor = 100;

        try {
            reader = new BufferedReader(new FileReader(Constants.APP_LOG_FILE));
            String line = reader.readLine();

            while (line != null) {

                String eventTime = line.split(" ")[0];

                DateFormat df = new SimpleDateFormat(Constants.SIMPLE_DATE_FORMAT);
                Date result =  df.parse(eventTime);
                long unixEventTime = result.getTime();

                if (previousUnixTime == null) {
                    previousUnixTime = unixEventTime;
                }

                Thread.sleep((unixEventTime - previousUnixTime) / speedFactor);

                ProducerRecord<String, String> record = new ProducerRecord<>(Constants.KAFKA_SOURCE_TOPIC, eventTime, line);

                myProducer.send(record).get();


                prettyPrint(line);

                // read next line
                line = reader.readLine();
            }
            reader.close();

        } catch (IOException | ParseException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        prettyPrint("FINISHED");

    }

    private static void prettyPrint(String message) {
        System.out.println(Constants.ANSI_PURPLE + "Kafka: " + message  + Constants.ANSI_RESET);
    }

    private Properties setupKafkaClientProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaProps;
    }


}
