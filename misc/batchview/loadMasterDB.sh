#! /bin/bash

DSE_SRV_IP=<dse_server_ip>
WORKLOAD_FILE=</path/to/workload_gen.csv>

dsbulk load \
   -h $DSE_SRV_IP \
   -url $WORKLOAD_FILE \
   -k master -t drillsensor_raw \
   -header false \
   -m "0=drill_id, 1=sensor_id, 2=sensor_type, 3=reading_date, 4=reading_time, 5=reading_value" \
   --codec.timestamp CQL_TIMESTAMP
