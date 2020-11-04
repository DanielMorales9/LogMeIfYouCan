package com.logmeifyoucan;

import com.logmeifyoucan.common.Parser;
import com.logmeifyoucan.generator.KafkaStreamDataGenerator;
import com.logmeifyoucan.helper.FlinkKafkaConsumerHelper;
import com.logmeifyoucan.helper.StreamExecutionEnvHelper;
import com.logmeifyoucan.model.LogPoint;
import com.logmeifyoucan.model.PageRequest;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Objects;

public class LoggingTimeScaleDB {

    private static final String URL = "jdbc:postgresql://localhost:5432/page_request";
    private static final String USERNAME = "mor0004d";
    private static final String PASSWORD = "password";

    public static void main(String[] args) {

        try {

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvHelper.createStreamExecutionEnvironment();

            //Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<String> kafkaConsumer = FlinkKafkaConsumerHelper.createConsumer();

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest();

            //Create the data stream
            DataStream<String> logStream = streamEnv.addSource(kafkaConsumer);


            //Convert each record to an Object
            DataStream<LogPoint> logPointStream = logStream
                    .map(Parser::parseLogMessage)
                    .filter(Objects::nonNull);

            DataStream<PageRequest> pageRequestStream = logPointStream
                    .filter(logPoint -> logPoint.getMsg().matches("Method: .*Resource: .*Duration: .*"))
                    .map(Parser::parseLogPoint)
                    .filter(p -> p != null)
                    .map(p -> {
                        LoggingTimeScaleDB.prettyPrint(p.toString());
                        return p;
                    });

            pageRequestStream.addSink(JdbcSink.sink(
                    "insert into public.measurements (event_time, page, method, duration) values (?,?,?,?)",
                    (ps, t) -> {
                        ps.setTimestamp(1, new Timestamp(t.getEventTime()));
                        ps.setString(2, t.getPage());
                        ps.setString(3, t.getMethod());
                        ps.setLong(4, t.getDuration());
                    },
                    new JdbcExecutionOptions.Builder()
                            .withBatchIntervalMs(10L)
                            .withBatchIntervalMs(1000)
                            .withMaxRetries(1)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(URL)
                            .withUsername(USERNAME)
                            .withPassword(PASSWORD)
                            .withDriverName("org.postgresql.Driver")
                            .build()));

            //Start the Kafka Stream generator on a separate thread
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Bulk loading into TimeScaleDB");
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    private static void prettyPrint(String message) {
        System.out.println("Flink: " + message);
    }

}
