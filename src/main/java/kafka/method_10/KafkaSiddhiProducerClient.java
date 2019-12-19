
/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package kafka.method_10;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;


/**
 * This is a sample TCP client to publish events to TCP endpoint.
 */
public class KafkaSiddhiProducerClient {
    private static final Logger log = Logger.getLogger(KafkaSiddhiProducerClient.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("Initialize kafka producer client.");
        final String[] types = new String[]{"json", "xml", "text"};

        String topic = !args[0].isEmpty() ? args[0] : "kafka_topic";
        String bootstrapServers = !args[1].isEmpty() ? args[1] : "localhost:9092";
        String partitionNo = !args[2].isEmpty() ? args[2] : null;
        String sequenceId = !args[3].isEmpty() ? args[3] : null;
        String key = !args[4].isEmpty() ? args[4] : null;
        String optionalConfiguration = !args[5].isEmpty() ? args[5] : null;

        String type = Arrays.asList(types).contains(args[6]) ? args[6] : "json";
        List<String[]> fileEntriesList = null;
        if (args.length >= 7 && !args[6].equals("")) {
            String filePath = args[6];
            fileEntriesList = readFile(filePath);
        }

        String eventDefinition;
        if (args.length >= 8 && !args[7].equals("")) {
            eventDefinition = args[7];
        } else {
            if (!args[7].equals("")) {
                if (type.equals("json")) {
                    eventDefinition = "{\"item\": {\"id\":\"{0}\",\"amount\": {1}}}";
                } else if (type.equals("xml")) {
                    eventDefinition = "<events><item><id>{0}</id><amount>{1}</amount></item></events>";
                } else {
                    eventDefinition = "id:\"{0}\"\namount:{1}";
                }
            } else {
                if (type.equals("json")) {
                    eventDefinition = "{\"event\": {\"name\":\"{0}\",\"amount\": {1}}}";
                } else if (type.equals("xml")) {
                    eventDefinition = "<events><event><name>{0}</name><amount>{1}</amount></event></events>";
                } else {
                    eventDefinition = "name:\"{0}\"\namount:{1}";
                }
            }
        }



        SiddhiManager siddhiManager = new SiddhiManager();

        InputHandler sweetProductionStream;

        String[] sweetName2 = {"Cupcake", "Donut", "Eclair", "Froyo", "Gingerbread", "Honeycomb", "Ice",
                "Cream Sandwich", "Jelly Bean", "KitKat", "Lollipop", "Marshmallow"};

        StringBuilder builder = new StringBuilder("@App:name(\"KafkaSink\")\n" +
                                                          "define stream SweetProductionStream(name string, amount double);\n" +
                                                          "@sink(type='kafka',\n");

        builder.append("topic='" + topic + "',\n");
        if (partitionNo != null) {
            builder.append("partition.no='" + partitionNo + "',\n");
        }
        builder.append("bootstrap.servers='" + bootstrapServers + "',\n");
        if (sequenceId != null) {
            builder.append("sequence.id='" + sequenceId + "',\n");
        }
        if (key != null) {
            builder.append("key='" + key + "',\n");
        }
        if (optionalConfiguration != null) {
            builder.append("optional.configuration='" + optionalConfiguration + "',\n");
        }
        builder.append("@map(type='"+type+"'))\n" +
                           "define stream SweetProductionStreamAsJson(name string, amount double);\n" +
                           "@info(name='query1')\n" +
                           "from SweetProductionStream\n" +
                           "select name, amount\n" +
                           "insert into SweetProductionStreamAsJson;");


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(builder.toString());

        siddhiAppRuntime.start();
        sweetProductionStream = siddhiAppRuntime.getInputHandler("SweetProductionStream");

        Thread.sleep(2000);
        String[] sweetName = {"Cupcake", "Donut", "Éclair", "Froyo", "Gingerbread", "Honeycomb", "Ice",
                              "Cream Sandwich", "Jelly Bean", "KitKat", "Lollipop", "Marshmallow"};
        while (true) {
            String message;
            if (fileEntriesList != null) {
                Iterator iterator = fileEntriesList.iterator();
                while (iterator.hasNext()) {
                    String[] stringArray = (String[]) iterator.next();
                    for (int i = 0; i < stringArray.length; i++) {
                        message = eventDefinition.replace("{" + i + "}", stringArray[i]);
                        sweetProductionStream.send(new Object[]{message});
                    }
                }
            } else {
                int amount = ThreadLocalRandom.current().nextInt(1, 10000);
                String name = sweetName[ThreadLocalRandom.current().nextInt(0, sweetName.length)];
                message = eventDefinition.replace("{0}", name).replace("{1}", Integer.toString(amount));
                sweetProductionStream.send(new Object[]{message});
            }
            Thread.sleep(Long.parseLong(args[5]));
        }
//        Thread.sleep(2000);
//
//        siddhiAppRuntime.shutdown();
//
//        Thread.sleep(2000);
    }

    private static List<String[]> readFile(String fileName) throws IOException {
        File file = new File(fileName);
        Scanner inputStream = new Scanner(file);
        List<String[]> fileEntriesList = new ArrayList<String[]>();
        while (inputStream.hasNext()) {
            String data = inputStream.next();
            fileEntriesList.add(data.split(","));
        }
        inputStream.close();
        return fileEntriesList;
    }


}