package com.logmeifyoucan.helper;

import com.logmeifyoucan.LoggingInfluxDB;
import com.logmeifyoucan.common.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConsumerHelper {

    public static FlinkKafkaConsumer<String> createConsumer() {
        //Set connection properties to Kafka Cluster
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", LoggingInfluxDB.GROUP_ID);

        //Setup a Kafka Consumer on Flink
        return new FlinkKafkaConsumer<>(Constants.KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), properties);
    }
}
