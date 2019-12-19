
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
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This is a sample TCP client to publish events to TCP endpoint.
 */
public class KafkaSiddhiConsumerClient {
    private static final Logger log = Logger.getLogger(KafkaSiddhiConsumerClient.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("Initialize kafka client.");
        SiddhiManager siddhiManager = new SiddhiManager();

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream BarStream (name string, amount double); " +
                        "@info(name = 'query1') " +
                        "@source(type='kafka', topic.list='kafka_sample', group.id='test_single_topic', " +
                        "threading.option='single.thread', bootstrap.servers='localhost:9092'," +
                        "@map(type='json'))" +
                        "Define stream FooStream (name string, amount double);" +
                        "from FooStream select name, amount insert into BarStream;");

        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    System.out.println("Received: ");
                    System.out.println(event);
                }
            }
        });

        Thread.sleep(60000);
    }


}