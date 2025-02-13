#!/bin/bash

# 환경 변수 설정
ZOOKEEPER_CONNECT=${ZOOKEEPER_CONNECT:-zookeeper:2181}
KAFKA_BROKER_ID=${KAFKA_BROKER_ID:-1}
KAFKA_PORT=${KAFKA_PORT:-9092}
KAFKA_HOST_NAME=${KAFKA_HOST_NAME}
KAFKA_ADVERTISED_PORT=${KAFKA_ADVERTISED_PORT:-9092}
KAFKA_ADVERTISED_HOST_NAME=${KAFKA_ADVERTISED_HOST_NAME}

# `kafka.properties` 파일 수정
sed -i "s/^zookeeper.connect=.*$/zookeeper.connect=${ZOOKEEPER_CONNECT:-127.0.0.1}/" /var/lib/kafka/conf/kafka.properties
sed -i "s/^broker.id=.*$/broker.id=${KAFKA_BROKER_ID}/" /var/lib/kafka/conf/kafka.properties
# sed -i "s/^listeners=.*$/listeners=SASL_PLAINTEXT:\/\/${KAFKA_HOST_NAME}:${KAFKA_PORT}/" /var/lib/kafka/conf/kafka.properties
# sed -i "s/^advertised.listeners=.*$/advertised.listeners=SASL_PLAINTEXT:\/\/${KAFKA_ADVERTISED_HOST}:${KAFKA_ADVERTISED_PORT}/" /var/lib/kafka/conf/kafka.properties
# sed -i "s/^advertised.listeners=.*$/advertised.listeners=SASL_PLAINTEXT:\/\/${KAFKA_ADVERTISED_HOST}:${KAFKA_ADVERTISED_PORT}/,SASL_PLAINTEXT:\/\/host.docker.internal:${KAFKA_ADVERTISED_PORT}/" /var/lib/kafka/conf/kafka.properties
# sed -i "s/^advertised.host.name=.*$/advertised.host.name=${KAFKA_ADVERTISED_HOST:-127.0.0.1}/" /var/lib/kafka/conf/kafka.properties

/opt/kafka_2.12-2.1.0/bin/kafka-server-start.sh /var/lib/kafka/conf/kafka.properties
