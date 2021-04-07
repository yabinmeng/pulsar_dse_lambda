#! /bin/bash

pulsar-admin sinks create \
    --name sensor_warning_cass_sink \
    --sink-config-file `pwd`/dse_sink.yaml \
    --sink-type cassandra-enhanced \
    --tenant public \
    --namespace default \
    --inputs persistent://public/default/warning_sensor_data