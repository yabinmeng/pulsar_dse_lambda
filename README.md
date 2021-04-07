# Overview

[Lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture) is a popular data-processing architecture pattern in addressing the challenges (such as latency, throughput, and latency) in the world of ***BIG*** data (or data lake). In this repository, I'm demonstrating how to implement the lambda architecture for a (simulated) IoT use case with several industry leading technologies that are best known for their capabilities in dealing with large amounts of data with large scale and high performance, either for batch-oriented data processing or real-time data processing.
* [Apache Cassandra (C*)](https://cassandra.apache.org/)
* [Apache Spark (Spark)](http://spark.apache.org/)
* [Apache Pulsar (Pulsar)](https://pulsar.apache.org/)

The high level architecture diagram of this implementation is as below:

![lambda architecture](resources/lambda_architecture.png)

By the above diagram,

* C* is used as the underlying storage mechanism for both the batch layer and the serving layer.
* Spark is used to do data loading and processing in batch mode from the batch layer to the serving layer.
* Pulsar is used as the underlying data processing and storage meachnism at the speed layer. 
  * Pulsar function can be used for more complext realtime stream processing.
  * Pulsar Cassandra connector can be used to land the processed realtime stream data into the serving layer.
* Through Spark SQL, the data of different views (batch and speed) and be queried by the end users in a powerful, robust yet flexible way.

## IoT Use Case Overview

For demo purposes, this repository uses an imaginary Oil Well drilling sensor IoT use case. In this super simplified use case,
* Each drilling site can have multiple drills
* Each drill has 2 types of sensors. One for measuring drill temperature and another for measuring drill speed
* The drill sensor data is constantly being collected at certain frequency (e.g. 1 second or 1 minute) and the collected data is fed into both the batch layer and the speed layer.

For the batch layer, the raw sensor data will be processed daily in order to generate different batch views for downstream analytical purposes. In this demo, one specific batch view is created for the following purpose:
* Get the average temperature and speed of all drills for every day.

For the speed layer, the raw sensor data will be processed in real time and only the messages of the most recent date will be kept because older data is already (or can be) reflected in some batch views. The speed layer will then do further processing (e.g. filtering, transforming, or aggregating) of these data and generate realtime views accordingly, depending on the downstream needs. In this demo, one specific realtime view is created for the following purpose:
* Get the list of the drills that are either too hot or spinning too fast for the current day.

### Raw Data Format

In this simplified use case, the raw drill sensor data has the following format, expressed in [Apache Avro](http://avro.apache.org/) format (*[raw_sensor_data.avsc](./misc/raw_sensor_data.avsc)*):

```
{
  "type": "record",
  "name": "IotSensor",
  "namespace": "TestNS",
  "fields" : [
    {"name": "DrillID", "type": "string"},
    {"name": "SensorID", "type": "string"},
    {"name": "SensorType", "type": "string"},
    {"name": "ReadingTime", "type": "string"},
    {"name": "ReadingValue", "type": "float"}
  ]
}
```

---

# Data Schema

In this demo, the following data schema (C* and Pulsar) is used for the above use case.

## C* Schema

There are 3 C* tables needed for this demo. The CQL keyspace and table definition (*[drill_sensor.cql](./misc/drill_sensor.cql)*) is as bleow:

```
// Batch Layer - master DB
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


// Serving Layer - batch view
CREATE KEYSPACE IF NOT EXISTS batchview WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE IF NOT EXISTS batchview.drill_info_by_date (
    drill_id text,
    reading_date date,
    sensor_type text,
    avg_value float,
    PRIMARY KEY ((drill_id, reading_date, sensor_type))
);


// Serving Layer - realtime view
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
```

## Pulsar Schema 

Pulsar has native schema support. In this demo, we're using the following Avro schema format (*[warning_sensor_data.avsc](./misc/warning_sensor_data.avsc)*) for the generated realtime view data.

```
{
  "type": "record",
  "name": "IotSensor",
  "namespace": "TestNS",
  "fields" : [
    {"name": "DrillID", "type": "string"},
    {"name": "SensorID", "type": "string"},
    {"name": "ReadingDate", "type": "string"},
    {"name": "ReadingTime", "type": "string"},
    {"name": "SensorType", "type": "string"},
    {"name": "ReadingValue", "type": "float"}
  ]
}
```

---

# Program Overview

In this demo, there are several programs/utilities that altogther form a complete end-to-end data processing flow following the lambda architecture:

| Item | Program | Description | Note | Location |
| ---- | ------- | ----------- | ---- | -------- |
| 1. | Workload Simulator | Generates a set of simulated drill sesnor data in CSV format | Data Source | [workload_generator](./workload_generator) |
| 2. | Data bulk loading utility (*) | Load the raw sensor data into the raw data master DB | External utility |[DataStax Bulk Loader](https://docs.datastax.com/en/dsbulk/doc/index.html) |
| 3. | Pulsar producer | Publish the raw sensor data to a Pulsar topic | Speed layer | [realtime_view/pulsar_producer](./realtime_view/pulsar_producer) |
| 4. | Pulsar function | Further realtime stream processing to generate the realtime view | Speed layer | [realtime_view/pulsar_function](./realtime_view/pulsar_function) |
| 5. | Daily batch job | Daily ETL job to generate the batch view from the raw data master DB | Batch layer | [batch_view/daily_batch](./batch_view/daily_batch) |

**NOTE**: Other than the data loading utility (item 2), all other programs are custom made for the purpose of this demo.


With these programs and utilities, the high level end-to-end data processing flow is as below:

## Pre-step: Create C* Keyspace and Table
```
$ cqlsh -f drill_sensor.cql
```

## Step 1: Generate simulated workload file

The main program, **WorkloadGen**, used for generating the simulated workload file takes the following input parameters:

```
usage: WorkloadGen [-f <arg>] [-h] [-o <arg>]

WorkloadGen:
  -f,--config <arg> Configuration properties file.
  -h,--help         Displays this help message.
  -o,--output <arg> Output CSV file name.
```

Among these paramters, *-f/--config* specifies the configuration property file locaiont that controls how the drill sensor data is generated, which has the following configuration properties:
* drill_num: the total number of drills
* sensor_types: the list of sensor types (separated by ',')
* sensor_num_per_type: the number of sensors per type
* workload_frequency: the frequency of one batch of the sample data records being generated. One batch of the sample data covers all sensors under all types for all drills.
* workload_period: the total time range that the sample data will be generated
* workload_enddate: the end date of the simulated workload

An example file (*generator.properties*) can be found [here](./workload_generator/src/main/resources/generator.properties). 

An example of running this proram to generate a workload file is as below:

```
$ java -jar build/libs/Workload_Generator-1.0-SNAPSHOT-all.jar -f </path/to/generator.properties> -o </path/to/workload_gen.csv>

```

## Step 2: Load the source data into the batch layer and the speed layer

## Step 3: Run batch job to generate the batch view

## Step 4-1: Deploy Pulsar function for realtime stream processing

## Step 4-2: Deploy Pulsar Cassandra sink connector to generate the realtime view
