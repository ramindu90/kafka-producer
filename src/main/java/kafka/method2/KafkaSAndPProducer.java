package kafka.method2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ramindu on 3/3/17.
 */
public class KafkaSAndPProducer {
    public static void main2(String[] args) {
        try {
            KafkaSAndPProducer app = new KafkaSAndPProducer();
            File myObj = app.getFileFromResource("publication-filter.json");
            Scanner myReader = new Scanner(myObj);
            StringBuilder data = new StringBuilder();
            while (myReader.hasNextLine()) {
                data.append(myReader.nextLine());
            }
            String noSlashes = data.toString().replace("\\u003d", "=");
            noSlashes = noSlashes.replace("\\u0027", "'");
            noSlashes = noSlashes.replace("\\n", "\n");
            noSlashes = noSlashes.replace("\\t", "\t");
            noSlashes = noSlashes.replace("\\", "");
            myReader.close();
            System.out.println(noSlashes);
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        AtomicLong totalRecordCount = new AtomicLong(0);
        long durationInMillies;
        if (args.length >= 1) {
            durationInMillies = Long.parseLong(args[0])*60*1000;
        } else {
            durationInMillies = 1*60*1000;
        }

        long thoughputEvents;
        if (args.length >= 2) {
            thoughputEvents = Long.parseLong(args[1]);
        } else {
            thoughputEvents = 500;
        }

        String bootstrapServer;
        if (args.length >= 3) {
            bootstrapServer = args[2];
        } else {
            bootstrapServer = "localhost:9092";
        }

        String topic;
        if (args.length >= 4) {
            topic = args[3];
        } else {
            topic = "sandpglobal";
        }

        long totalNumber;
        if (args.length >= 5) {
            totalNumber = Integer.parseInt(args[4]);
        } else {
            totalNumber = 100000;
        }

        int partition;
        if (args.length >= 6) {
            partition = Integer.parseInt(args[5]);
        } else {
            partition = -1;
        }

        String payload = "{  \"schemaVersion\": \"v1.0\",  \"id\": \"8ea3bb63-6ca1-4eda-9e39-84218d3b5ad5\",  \"contentGroup\": \"Binary Package\",  \"contentType\": \"Package\",  \"content\": [],  \"instanceIdentifier\": \"4c080d8f-6fd7-4aa2-81b4-c904eae0c02a\",  \"commodities\": [\"Natural Gas\",\"Oil\"],  \"commoditiesV2\": [\"Natural Gas\",\"Oil\"],  \"geographies\": [\"North America\",\"Asia\"],  \"pricingRegions\": [],  \"companies\": [],  \"subjectAreas\": [],  \"contentStatus\": \"Published\",  \"copyright\": [],  \"name\": \"GD_20200727.pdf\",  \"mghId\": \"4c080d8f-6fd7-4aa2-81b4-c904eae0c02a\",  \"sourceId\": {    \"sourceSystem\": \"PEN\",    \"text\": \"8ea3bb63-6ca1-4eda-9e39-84218d3b5ad5\"  },  \"mimeType\": \"application/pdf\",  \"filedBy\": \"SYS-PLATTSHARMONY-PROD\",  \"owner\": \"kenni_padmore\",  \"createdDate\": \"2020-07-25T00:43:58.903Z\",  \"publications\": [\"Gas Daily\"],  \"updatedDate\": \"2020-07-25T00:59:24.689Z\",  \"language\": \"English\",  \"source\": [],  \"branding\": [],  \"rights\": [],  \"richMediaImageTitle\": [],  \"userIn\": [],  \"packageShortCode\": \"GD\",  \"coverDate\": \"2020-07-27T00:00:00Z\",  \"byteSize\": \"1956856\",  \"referenceId\": \"/company_home/Platts/Package/Gas Daily/20200727/GD_20200727_d072061c-f787-40d3-b00f-5924951ce86d.pdf\"}\n";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        long endTime = System.currentTimeMillis() + durationInMillies;
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        long thoughputStartTime = System.currentTimeMillis();
        AtomicLong numOfEvents = new AtomicLong(0);
        while (endTime > System.currentTimeMillis()) {
            if (totalNumber == totalRecordCount.get()) {
                System.out.println("Sent " + numOfEvents + " per second. totalRecordCount: " + totalRecordCount.get());
                break;
            }
            numOfEvents.incrementAndGet();
            totalRecordCount.incrementAndGet();
            if (numOfEvents.get() == thoughputEvents || thoughputStartTime + 1000 < System.currentTimeMillis()) {
                System.out.println("Sent " + numOfEvents + " per second. totalRecordCount: " + totalRecordCount.get());
                if (thoughputStartTime + 1000 > System.currentTimeMillis()) {
                    try {
                        Thread.sleep((thoughputStartTime + 1000) - System.currentTimeMillis());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                thoughputStartTime = System.currentTimeMillis();
                numOfEvents = new AtomicLong(0);
            }
            if (partition != -1) {
                producer.send(new ProducerRecord<String, String>(topic, partition,
                        System.currentTimeMillis(), null, payload));
            } else {
                producer.send(new ProducerRecord<String, String>(topic, null,
                        System.currentTimeMillis(), null, payload));
            }

        }
        producer.close();
    }

    // get a file from the resources folder
    // works everywhere, IDEA, unit test and JAR file.
    private InputStream getFileFromResourceAsStream(String fileName) {

        // The class loader that loaded the class
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        // the stream holding the file content
        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return inputStream;
        }

    }

    /*
        The resource URL is not working in the JAR
        If we try to access a file that is inside a JAR,
        It throws NoSuchFileException (linux), InvalidPathException (Windows)

        Resource URL Sample: file:java-io.jar!/json/file1.json
     */
    private File getFileFromResource(String fileName) throws URISyntaxException {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return new File(resource.toURI());
        }

    }

    // print input stream
    private static void printInputStream(InputStream is) {

        try (InputStreamReader streamReader =
                     new InputStreamReader(is, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
