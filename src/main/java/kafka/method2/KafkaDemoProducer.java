package kafka.method2;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by ramindu on 3/3/17.
 */
public class KafkaDemoProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class", "kafka.method1.SimplePartitioner");

        long endTime = System.currentTimeMillis() + 1800000;
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int i = 0;
//        while (endTime > System.currentTimeMillis()) {
        while (i !=0) {
            i++;
//            String msg = "{\"symbol\":\"WSO2\",\"price\":56.75,\"volume\":" + i + "}";
//            msg = "wso2,12.5," + i;
            String msg = "{\"event\": {\"name\": \"wso2id" + i + "\", \"amount\":123.22}}";
//            System.out.println(msg);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            producer.send(new ProducerRecord<String, String>("kafka_topic", String.valueOf(i%2), msg));
//            producer.send(new ProducerRecord<String, String>("single_topic_demo", Integer.toString(2), msg));
//            producer.send(new ProducerRecord<String, String>("kafka_sample_0002", msg));
//            producer.send(new ProducerRecord<String, String>("kafka_sample_0002", msg), new Callback() {
//                @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    System.out.println(exception.getMessage() + ", " + metadata.0ffset());
//                }
//            });
            System.out.println(msg);
            producer.send(new ProducerRecord<String, String>("sandpglobal", 0, null, msg));
        }
        producer.close();
    }
}
