package kafka.method2;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by ramindu on 3/3/17.
 */
public class ConsumerThread implements Runnable {

    private final KafkaConsumer<byte[], byte[]> consumer;
    private String evento;
    private static final Logger log = Logger.getLogger(ConsumerThread.class);
    private int threadNumber;

    public ConsumerThread(String topic, String partitionList, Properties props, int threadNumber) {
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
        props.put("bootstrap.servers", "localhost:9093");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        String topic = "kafka_sample";
//        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(props);
//        String topic2 = "kafka_output";
//        KafkaConsumer<String, String> consumer3 = new KafkaConsumer<String, String>(props);
//        String topic3 = "kafka_result";
        List<TopicPartition> partitionsList = new ArrayList<TopicPartition>();
        TopicPartition partition = new TopicPartition(topic, 0);
        partitionsList.add(partition);
//        partition = new TopicPartition(topic, 1);
//        partitionsList.add(partition);
//        partition = new TopicPartition(topic, 2);
//        partitionsList.add(partition);

        consumer.assign(partitionsList);
//        consumer.subscribe(Arrays.asList(topic));
//        consumer2.subscribe(Arrays.asList(topic2));
//        consumer3.subscribe(Arrays.asList(topic3));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("kafka_sample\t");
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
//            ConsumerRecords<String, String> records2 = consumer2.poll(100);
//            for (ConsumerRecord<String, String> record : records2) {
//                System.out.printf("kafka_output\t");
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//            }
//            ConsumerRecords<String, String> records3 = consumer3.poll(100);
//            for (ConsumerRecord<String, String> record : records3) {
//                System.out.printf("kafka_result\t");
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//            }
//            ConsumerRecords<String, String> records4 = consumer3.poll(100);
//            for (ConsumerRecord<String, String> record : records4) {
//                System.out.printf("kafka_result\t");
//                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
//            }
        }
    }

}
