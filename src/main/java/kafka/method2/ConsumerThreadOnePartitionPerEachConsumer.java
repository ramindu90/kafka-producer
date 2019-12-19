package kafka.method2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by ramindu on 3/3/17.
 */
public class ConsumerThreadOnePartitionPerEachConsumer implements Runnable {

    private final KafkaConsumer<byte[], byte[]> consumer;
    private String evento;
    private static final Logger log = Logger.getLogger(ConsumerThreadOnePartitionPerEachConsumer.class);
    private int threadNumber;

    public ConsumerThreadOnePartitionPerEachConsumer(String topic, String partitionList, Properties props, int threadNumber) {
        System.out.println("consumer thread created");
        this.consumer = new KafkaConsumer<byte[], byte[]>(props);
        String partitions[] = partitionList.split(",");
        List<TopicPartition> partitionsList = new ArrayList<TopicPartition>();
        for (String partition1 : partitions) {
            TopicPartition partition = new TopicPartition(topic, Integer.parseInt(partition1));
            partitionsList.add(partition);
        }
        System.out.println(partitionsList.toString());
        consumer.assign(partitionsList);
        this.threadNumber = threadNumber;
    }

    @Override
    public void run() {
        System.out.println("consumer started");
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(200);
            for (ConsumerRecord record : records) {
                System.out.printf("offset = %d, key = %s, value = %s, partition = %s thread = %d\n", record.offset(), record.key().toString(), record.value(), record.partition(), threadNumber);
                evento = record.value().toString();
//                if (log.isDebugEnabled()) {
//                    log.debug("Event received in Kafka Event Adaptor: " + evento + ", offSet: " + record.offset() + ", key: " + record.key() + ", partition: " + record.partition());
//                }
            }
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093,localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "kafka_topic_999";

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<TopicPartition> partitionsList = new ArrayList<TopicPartition>();
        partitionsList.add(new TopicPartition(topic, 0));
        consumer.assign(partitionsList);

//        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(props);
//        List<TopicPartition> partitionsList2 = new ArrayList<TopicPartition>();
//        partitionsList2.add(new TopicPartition(topic, 1));
//        consumer2.assign(partitionsList2);
//
//        KafkaConsumer<String, String> consumer3 = new KafkaConsumer<String, String>(props);
//        List<TopicPartition> partitionsList3 = new ArrayList<TopicPartition>();
//        partitionsList3.add(new TopicPartition(topic, 2));
//        consumer3.assign(partitionsList3);
//
//        KafkaConsumer<String, String> consumer4 = new KafkaConsumer<String, String>(props);
//        List<TopicPartition> partitionsList4 = new ArrayList<TopicPartition>();
//        partitionsList4.add(new TopicPartition(topic, 3));
//        consumer4.assign(partitionsList4);



        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("kafka_topic_999 0 partition\t");
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
//            ConsumerRecords<String, String> records2 = consumer2.poll(100);
//            for (ConsumerRecord<String, String> record : records2) {
//                System.out.printf("kafka_topic_999 1 partition\t");
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//            }
//            ConsumerRecords<String, String> records3 = consumer3.poll(100);
//            for (ConsumerRecord<String, String> record : records3) {
//                System.out.printf("kafka_topic_999 2 partition\t");
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//            }
//            ConsumerRecords<String, String> records4 = consumer3.poll(100);
//            for (ConsumerRecord<String, String> record : records4) {
//                System.out.printf("kafka_topic_999 3 partition\t");
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//            }
        }
    }

}
