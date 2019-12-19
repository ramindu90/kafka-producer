package kafka.method1;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    private final int partitionNum;

    private static final Logger log = Logger.getLogger(ConsumerGroupExample.class);

    public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic, int partitionNum) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
        this.partitionNum = partitionNum;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
        log.info("Kafka consumer: " + this.partitionNum + " started.");

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber, partitionNum));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("session.timeout.ms", "400");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
        String zooKeeper = "localhost:9093";
        String groupId = "group1";
        String topic = "kafka_sample";
        int threads = 2;
        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic, 0);
        example.run(threads);
//        ConsumerGroupExample example2 = new ConsumerGroupExample(zooKeeper, groupId, topic, 1);
//        example2.run(threads);
        try {
            Thread.sleep(600000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();
//        example2.shutdown();
    }
}


