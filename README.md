# Kafka Connect plugin

Kafka topics can only be read by a 1:1 ratio of consumer group to
partition. A topic with four partitions has a maximum of four distinct
consumers within a consumer group (additional will not be assigned
a partition unless a rebalance occurs). If the consumer performs
operations which cause it unable to process messages immediately,
lag will begin to grow. Since you cannot arbitrarily scale
horizontally, this Kafka Connect plugin will fan out a topic
into multiple "sub-topics". This allows more parallel consumers,
allowing processing of more messages simultaneously.

Note that this plugin has been updated from Kafka 3.7.x to 4.1.x,
requiring a Kafka 4 cluster.

### connect-standalone.properties
```properties
bootstrap.servers=localhost:9092

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter

key.converter.schemas.enable=false
value.converter.schemas.enable=false
# The above were originally "true", but we have no schemas

offset.storage.file.filename=/tmp/connect.offsets

offset.flush.interval.ms=5000
```

### traffic-mon-connect.properties
```properties
name=roundrobin-fanout
connector.class=org.apache.kafka.connect.mirror.MirrorSourceConnector
tasks.max=4
topics=traffic-topic
source.cluster.alias=main
target.cluster.alias=fanout

transforms=RoundRobin
transforms.RoundRobin.type=net.lavacro.kafka.connect.RoundRobinTopic
transforms.RoundRobin.target.topic.base=traffic-topic
transforms.RoundRobin.num.topics=4

replication.factor=1

source.cluster.bootstrap.servers=localhost:9092
target.cluster.bootstrap.servers=localhost:9092

consumer.auto.offset.reset=latest

key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false

value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

# producer tuning
producer.batch.size=524288
producer.linger.ms=20
producer.compression.type=lz4
producer.acks=1
```

### create subtopics
```shell
for topic in 00 01 02 03
do
  bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 4 \
    --topic traffic-topic-$topic
done
```