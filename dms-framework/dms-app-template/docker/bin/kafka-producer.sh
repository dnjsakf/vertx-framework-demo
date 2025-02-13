/opt/kafka_2.12-2.1.0/bin/kafka-console-producer.sh \
  --broker-list "${BROKER_LIST:-localhost:9092}" \
  --topic "$TOPIC_NAME" \
  --producer.config "/var/lib/kafka/conf/auth/consumer2.properties"
