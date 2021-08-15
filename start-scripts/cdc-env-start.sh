#!/bin/sh

~/gsb-apps/spark-3.1.1-bin-hadoop2.7/sbin/start-all.sh &
#sudo docker start postgresql-container
~/gsb-apps/kafka_2.13-2.8.0/bin/zookeeper-server-start.sh /home/ashen/gsb-apps/kafka_2.13-2.8.0/config/zookeeper.properties &
~/gsb-apps/kafka_2.13-2.8.0/bin/kafka-server-start.sh /home/ashen/gsb-apps/kafka_2.13-2.8.0/config/server.properties &
/home/ashen/gsb-apps/kafka_2.13-2.8.0/bin/connect-standalone.sh /home/ashen/gsb-apps/kafka_2.13-2.8.0/config/connect-standalone.properties /home/ashen/gsb-apps/kafka_2.13-2.8.0/config/connector1.properties &
