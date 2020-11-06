CREATE TABLE measurements (
    event_time timestamp without time zone NOT NULL,
    page character varying(255) NOT NULL,
    method character varying(255) NOT NULL,
    duration bigint NOT NULL
);

SELECT create_hypertable('measurements', 'event_time');