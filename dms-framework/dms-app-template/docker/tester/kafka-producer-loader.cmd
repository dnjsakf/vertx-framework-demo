@echo off

pushd %~dp0

docker cp %CD%/sample.data kafka-01:/tmp/sample.data

docker exec -i -e BROKER_LIST="kafka-01:9092" kafka-01 ^
  /bin/bash -c "cat /tmp/sample.data | /opt/kafka_2.12-2.1.0/bin/kafka-console-producer.sh --broker-list ^"kafka-01:9092^" --topic ^"topic-navilog2-acs-post-json^" --producer.config ^"/var/lib/kafka/conf/auth/consumer2.properties^""

popd
