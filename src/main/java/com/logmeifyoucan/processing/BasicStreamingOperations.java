package com.logmeifyoucan.processing;

import com.logmeifyoucan.common.Constants;
import com.logmeifyoucan.LogPoint;
import com.logmeifyoucan.generator.KafkaStreamDataGenerator;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BasicStreamingOperations {

    public static final String GROUP_ID = "flink.learn.realtime";

    public static void main(String[] args) {

        try {

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
            properties.setProperty("group.id", GROUP_ID);

            //Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>(Constants.KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), properties);

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest();

            //Create the data stream
            DataStream<String> logStream = streamEnv.addSource(kafkaConsumer);

            //Convert each record to an Object
            DataStream<LogPoint> logPointStream = logStream
                    .map(BasicStreamingOperations::parseLogMessage)
                    .filter(Objects::nonNull)
                    .map(s -> {
                        prettyPrint(s.toString());
                        return s;
                    });

            //Start the Kafka Stream generator on a separate thread
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Log Analysis");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    private static LogPoint parseLogMessage(String logMessage) throws ParseException {
        Pattern p = Pattern.compile(Constants.REGEX);
        Matcher m = p.matcher(logMessage);

        if (m.find() && m.groupCount() >= 7) {
            String logDate = m.group(1);
            String eventDate = m.group(2);
            String level = m.group(3);
            String thread = m.group(4);
            String pid = m.group(5);
            String className = m.group(6);
            String msg = m.group(7);

            DateFormat df;
            df = new SimpleDateFormat(Constants.SIMPLE_DATE_FORMAT);
            long logTime = df.parse(logDate).getTime();
            df = new SimpleDateFormat(Constants.SIMPLE_DATE_FORMAT2);
            long eventTime = df.parse(eventDate).getTime();

            return new LogPoint(logTime, eventTime, level, thread, pid, className, msg);
        }
        else {
            prettyPrint("ERROR: " + logMessage);
            return null;
        }


    }

    private static void prettyPrint(String message) {
        System.out.println("Flink: " + message);
    }


}
