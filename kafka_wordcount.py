
"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: kafka_wordcount.py <zk> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 Steps followed:
 Go to kafka/ directory
 1. Start zookeeper
   `bin/zookeeper-server-start.sh config/zookeeper.properties`
 2. Now start the Kafka server:
    `bin/kafka-server-start.sh config/server.properties`
 3. Create a topic named "test"
    `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`
 4. Run the producer and then type a few messages into the console to send to the server.
    `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test`
 5. Start spark to consume messages. Copy spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar and kafka_wordcount.py to usr/local/spark/bin/ and then  run from usr/local/spark/bin/ 
    `./spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar kafka_wordcount.py localhost:2181 test`

"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    refreshTime = StreamingContext(sc, 20)

    zkQuorum, topic = sys.argv[1:]
    kafka_consumer = KafkaUtils.createStream(refreshTime, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kafka_consumer.map(lambda x: x[1])
    word_count = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    def print_function(rdd):
        print("---- %s ----"%str(rdd.collect()))
        print("\n")

    word_count.foreachRDD(print_function)
    refreshTime.start()
    refreshTime.awaitTermination()

