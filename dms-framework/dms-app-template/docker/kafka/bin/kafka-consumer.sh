/opt/kafka_2.12-2.1.0/bin/kafka-console-consumer.sh \
  --bootstrap-server "${BROKER_LIST:-localhost:9092}" \
  --topic "$TOPIC_NAME" \
  --consumer.config "/var/lib/kafka/conf/auth/consumer1.properties" \
  --from-beginning \
  $@
