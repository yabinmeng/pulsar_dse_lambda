#! /bin/bash

pulsar-admin functions create \
  --name SensorWarningFilter \
  --jar <path/to/SensorWarningFilter-1.0-SNAPSHOT-all.jar> \
  --classname com.example.SensorWarnFilterFunc \
  --auto-ack true \
  --inputs persistent://public/default/raw_sensor_data \
  --output persistent://public/default/warning_sensor_data \
  --log-topic persistent://public/default/sensor_warning_filter_log