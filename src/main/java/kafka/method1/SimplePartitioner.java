package kafka.method1;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ramindu on 1/11/17.
 */
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner () {

    }

    public int partition(Object partitionObject, int a_numPartitions) {
        System.out.println(partitionObject.toString() + " - " + a_numPartitions);
        int partitionNumber = (Integer) partitionObject;
        return partitionNumber % a_numPartitions;
    }

    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
//        System.out.println(s);
//        System.out.println(o.toString());
//        System.out.println(cluster.availablePartitionsForTopic(s).size());
        int partitionNumber = Integer.valueOf(o.toString());
        return partitionNumber % cluster.availablePartitionsForTopic(s).size();
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}