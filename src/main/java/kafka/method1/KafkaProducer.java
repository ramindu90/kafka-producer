package kafka.method1;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;

import java.util.*;

/**
 * Created by ramindu on 1/9/17.
 */
public class KafkaProducer {
    public static void main2(String[] args) {
        long events = 1l;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

//        ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
//        AdminUtils.createTopic(zkClient, "page_visits", 10, 1, new Properties());
//        zkClient.close();

        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = "{\"symbol\":\"WSO2\",\"price\":56.75,\"volume\":5}";
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("receiver_topic", "HAIOOOOOOO,12.5,100");
            producer.send(data);
        }
        producer.close();
    }

    private static Scanner in;

    public static void main(String[] argv)throws Exception {

//        kafka.method1.PartitionClient partitionClient = new kafka.method1.PartitionClient();
//        partitionClient.addPartitions("localhost:2181", "page_visits", 3);

        long events = 100l;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("request.required.acks", "1");
        props.put("metadata.broker.list","localhost:9092, localhost:9093");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.method1.SimplePartitioner");
//        props.put("partition.1","USA");
//        props.put("partition.2","India");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 1; nEvents < events; nEvents++) {
            String msg = "{\"symbol\":\"WSO2\",\"price\":56.75,\"volume\":"+nEvents+"}";
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("kafka_topic", msg);
            System.out.println(nEvents);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("kafka_topic", String.valueOf(nEvents), msg);
            producer.send(data);
        }
        producer.close();
    }
}
