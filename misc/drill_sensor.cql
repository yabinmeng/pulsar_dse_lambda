// Master DB
CREATE KEYSPACE IF NOT EXISTS master WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE IF NOT EXISTS master.drillsensor_raw (
    drill_id text,
    sensor_id text,
    reading_date date,
    reading_time timestamp,
    sensor_type text static,
    reading_value float,
    PRIMARY KEY ((drill_id, sensor_id, reading_date), reading_time)
);


// Batch View
CREATE KEYSPACE IF NOT EXISTS batchview WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE IF NOT EXISTS batchview.drill_info_by_date (
    drill_id text,
    reading_date date,
    sensor_type text,
    avg_value float,
    PRIMARY KEY ((drill_id, reading_date, sensor_type))
);


// Realtime View
CREATE KEYSPACE IF NOT EXISTS realtimeview WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE IF NOT EXISTS realtimeview.drill_warning_today (
    drill_id text,
    sensor_id text,
    reading_date date,
    reading_time timestamp,
    sensor_type text,
    reading_value float,
    PRIMARY KEY ((drill_id, sensor_id, reading_date), reading_time)
);