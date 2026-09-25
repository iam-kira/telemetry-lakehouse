-- Flink SQL client init: run at every SQL-client start so DDL isn't retyped.
-- The telemetry table reads the live Kafka `telemetry` topic (fed by sim -> Mosquitto -> bridge).
--
-- The WATERMARK line is the whole point of this file: it declares EVENT TIME.
-- `event_time - INTERVAL '5' SECOND` = "I'll wait up to 5 s for out-of-order events;
-- once I've seen an event at time T, any later event older than T-5s is treated as late."

SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE telemetry (
  device_id  STRING,
  ts         DOUBLE,          -- epoch seconds, written by the sensor
  temp_c     DOUBLE,
  door_open  BOOLEAN,
  seq        BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(CAST(ts * 1000 AS BIGINT), 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'telemetry',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
