minutesToProduce,TPS,brokerHostAndPort,topic,totalNumberOfEventsToPublish,partition

java -jar org.wso2.kafka.producer-1.0-SNAPSHOT-jar-with-dependencies.jar 10 20000 localhost:9092 testTopic 100000 -1