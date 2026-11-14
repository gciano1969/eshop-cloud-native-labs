#!/usr/bin/env bash
# Creates all the topics used up to Lab 4. Safe to run again: --if-not-exists.
set -euo pipefail
KT='oc rsh deploy/kafka /opt/kafka/bin'
create() {   # name partitions
  $KT/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}
create orders.events 3        # domain events of order-service (confirmed, cancelled), key = order id
create notification.dlq 1     # messages notification-service cannot process
create payment.commands 3     # Lab 4: commands to payment-service (authorize, void), key = order id
create payment.events 3       # Lab 4: replies of payment-service (authorized, rejected, voided)
create payment.dlq 1          # Lab 4: commands payment-service cannot process
create orders.dlq 1           # Lab 4: payment replies the saga consumer of order-service cannot process
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --list
