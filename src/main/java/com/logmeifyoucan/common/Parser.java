package com.logmeifyoucan.common;

import com.logmeifyoucan.LoggingInfluxDB;
import com.logmeifyoucan.common.Constants;
import com.logmeifyoucan.model.LogPoint;
import com.logmeifyoucan.model.PageRequest;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

    public static LogPoint parseLogMessage(String logMessage) throws ParseException {
        Pattern p = Pattern.compile(Constants.LOG_REGEX);
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
        } else {
            return null;
        }


    }

    public static PageRequest parseLogPoint(LogPoint logPoint) {
        String actualLogMessage = logPoint.getMsg();

        Pattern p = Pattern.compile("Method: (.*) Resource: (.*) Duration: (.*)");
        Matcher m = p.matcher(actualLogMessage);

        if (m.find() && m.groupCount() >= 3) {
            String method = m.group(1);
            String page = m.group(2);
            Long duration = Long.valueOf(m.group(3));
            return new PageRequest(logPoint.getEvenTime(), method, page, duration);
        }
        else {
            return null;
        }
    }
}
