package com.logmeifyoucan.common;

public class Constants {
    public static final String KAFKA_SOURCE_TOPIC = "flink.kafka.streaming.source";
    public static final String APP_LOG_FILE = "logs/app.log";
    public static final String SIMPLE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String SIMPLE_DATE_FORMAT2 = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String LOG_REGEX = "(\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d.\\d\\d\\dZ) +(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d.\\d\\d\\d) +(\\S+) +(\\d+) --- \\[(\\S+)\\] +(\\S+) +: +(.*)";
}
