package com.logmeifyoucan;

public class LogPoint {

    private final Long logTime;
    private final Long evenTime;
    private final String level;
    private final String pid;
    private final String thread;
    private final String className;
    private final String msg;

    public LogPoint(Long logTime, Long eventTime, String level, String pid, String thread, String className, String msg) {
        this.logTime = logTime;
        this.evenTime = eventTime;
        this.level = level;
        this.pid = pid;
        this.thread = thread;
        this.className = className;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "LogPoint{" +
                "logTime=" + logTime +
                ", evenTime=" + evenTime +
                ", level='" + level + '\'' +
                ", pid='" + pid + '\'' +
                ", thread='" + thread + '\'' +
                ", className='" + className + '\'' +
                ", msg='" + msg + '\'' +
                '}';
    }
}
