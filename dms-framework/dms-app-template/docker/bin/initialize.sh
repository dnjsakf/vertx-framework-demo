#!/bin/bash

# Kafka가 시작될 때까지 대기
sleep 5

# 플래그 파일 경로
FLAG_FILE="/var/lib/kafka/.setup_done"

# 플래그 파일이 존재하면 스크립트 종료
if [ -f "$FLAG_FILE" ]; then
  echo "Setup already done. Skipping."
  exit 0
fi

# 환경 변수에서 토픽 이름 가져오기
if [ -z "$TOPIC_NAME" ]; then
  echo "Error: TOPIC_NAME is not set. Please set the TOPIC_NAME environment variable."
  exit 1
fi

# 환경 변수에서 그룹 이름 가져오기
if [ -z "$GROUP_NAME" ]; then
  echo "Error: GROUP_NAME is not set. Please set the GROUP_NAME environment variable."
  exit 1
fi

# 환경 변수에서 사용자 이름 가져오기
IFS=',' read -r -a USERS <<< "$USER_NAMES"

# 환경 변수에서 사용자 역할 가져오기
IFS=',' read -r -a ROLES <<< "$USER_ROLES"

# 토픽 생성
/opt/kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper "$ZOOKEEPER_CONNECT" --replication-factor 1 --partitions 1 --topic "$TOPIC_NAME"

# 사용자별 ACL 설정
for index in "${!USERS[@]}"; do
  user="${USERS[$index]}"
  role="${ROLES[$index]}"

  KAFKA_ACLS_ARGS="--add --authorizer-properties zookeeper.connect=$ZOOKEEPER_CONNECT --allow-principal User:$user"
  if [ "$role" == "consumer" ]; then
    KAFKA_ACLS_ARGS="$KAFKA_ACLS_ARGS --consumer --group $GROUP_NAME"
  elif [ "$role" == "producer" ]; then
    KAFKA_ACLS_ARGS="$KAFKA_ACLS_ARGS --producer"
  elif [ "$role" == "admin" ]; then
    KAFKA_ACLS_ARGS="$KAFKA_ACLS_ARGS --operation All"
  fi
  KAFKA_ACLS_ARGS="$KAFKA_ACLS_ARGS --topic $TOPIC_NAME"

  /opt/kafka_2.12-2.1.0/bin/kafka-acls.sh $KAFKA_ACLS_ARGS

done

echo "Recevied!!!" | /opt/kafka_2.12-2.1.0/bin/kafka-console-producer.sh \
  --broker-list "$BOROKER_LIST" \
  --topic "$TOPIC_NAME" \
  --producer.config "/var/lib/kafka/conf/auth/consumer2.properties"

/opt/kafka_2.12-2.1.0/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BOROKER_LIST" \
  --topic "$TOPIC_NAME" \
  --consumer.config "/var/lib/kafka/conf/auth/consumer1.properties" \
  --from-beginning \
  --max-messages 1

# 플래그 파일 생성
touch "$FLAG_FILE"

