@echo off

pushd %~dp0

docker cp %CD%/consumer.group.properties kafka-01:/tmp/consumer.group.properties

docker exec -i kafka-01 ^
  /bin/bash /opt/kafka_2.12-2.1.0/bin/kafka-consumer-groups.sh ^
  --describe ^
  --bootstrap-server "localhost:9092" ^
  --group "acs-group" ^
  --command-config "/tmp/consumer.group.properties" ^
  --timeout 10000

popd
