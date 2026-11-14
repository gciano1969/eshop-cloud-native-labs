#!/usr/bin/env bash
# Creates the Lab 2 topics. Topics are created explicitly, never on the fly (auto-creation is off).
set -euo pipefail
KT='oc rsh deploy/kafka /opt/kafka/bin'
create() {   # name partitions
  $KT/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}
create orders.events 3        # domain events of order-service, key = order id
create notification.dlq 1     # messages notification-service cannot process
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.events
