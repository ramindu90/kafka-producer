package kafka.method1; /**
 * Created by ramindu on 1/13/17.
 */

import org.apache.log4j.Logger;

public class PartitionClient {

    private static final Logger logger = Logger.getLogger(PartitionClient.class);
    private static int sessionTimeout = 30000;
    private static int connectionTimeout = 6000;

    public PartitionClient() {
        // TODO Auto-generated constructor stub
    }

    public void addPartitions(String zkServers, String topic, int partitions) {

//        ZkClient zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout, ZKStringSerializer$.MODULE$);
//
//        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionReplicaList, zkClient, true);
//
//
//        try{
//            ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
//
//            if (AdminUtils.topicExists(zkUtils, topic)) {
//                logger.info("Altering topic {" + topic + "}");
//                try {
//                    AdminUtils.addPartitions(zkUtils, topic, partitions, "");
//                    logger.info("Topic {"+topic+"} altered with partitions : {"+partitions+"}");
//                } catch (AdminOperationException aoe) {
//                    logger.info("Error while altering partitions for topic : {"+topic+"}", aoe);
//                }
//            } else {
//                logger.info("Topic {"+topic+"} doesn't exists");
//            }
//            zkClient.close();
//        } finally {
//
//        }
    }
}
