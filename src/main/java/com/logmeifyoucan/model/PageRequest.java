package com.logmeifyoucan.model;

public class PageRequest {

    private final Long eventTime;
    private final String method;
    private final String page;
    private final Long duration;

    public PageRequest(Long evenTime, String method, String page, Long duration) {
        this.eventTime = evenTime;
        this.method = method;
        this.page = page;
        this.duration = duration;
    }

    public Long getDuration() {
        return duration;
    }

    public String getPage() {
        return page;
    }

    public String getMethod() {
        return method;
    }

    public Long getEventTime() {
        return eventTime;
    }

    @Override
    public String toString() {
        return "PageRequest{" +
                "eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", page='" + page + '\'' +
                ", duration=" + duration +
                '}';
    }
}
