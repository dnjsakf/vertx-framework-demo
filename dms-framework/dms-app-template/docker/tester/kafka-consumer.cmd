@echo off

pushd %~dp0

start docker exec -it -e BROKER_LIST="localhost:9092" kafka-01 ^
  /bin/bash /opt/kafka_2.12-2.1.0/bin/kafka-console-consumer.sh ^
  --bootstrap-server "localhost:9092" ^
  --topic "topic-navilog2-acs-post-json" ^
  --consumer.config "/var/lib/kafka/conf/auth/consumer1.properties" ^
  --from-beginning

popd

