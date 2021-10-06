#!/bin/sh

~/gsb-apps/kafka_2.13-2.8.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic postgres.public.customers

~/gsb-apps/kafka_2.13-2.8.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic postgres.public.orders