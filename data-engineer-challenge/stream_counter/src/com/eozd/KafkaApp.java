package com.eozd;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


@Command(name = "kafka_app", mixinStandardHelpOptions = true, version = "0.1", description = "Reads frames streamed in .jsonl format and produces unique user" + " counts for each minute in the data. The results are written both to STDOUT" + " and to a new Kafka topic.")
class KafkaApp implements Callable<Integer> {
    @Option(names="--broker", defaultValue = "localhost:9092", description = "Kafka server address (default: ${DEFAULT-VALUE})")
    private String broker;

    @Option(names="--report_period_sec", defaultValue = "5", description = "The time between two reports. This is used both for STDOUT" + " and for writing back to Kafka (default: ${DEFAULT-VALUE})")
    private int reportPeriodSec = 5;

    @Option(names="--jsonl_topic", defaultValue = "kafka_data", description = "Topic name of the frame data (default: ${DEFAULT-VALUE})")
    private String jsonlTopic;

    @Option(names="--stats_topic", defaultValue = "stats", description = "Topic name of the output statistics (default: ${DEFAULT-VALUE})")
    private String statsTopic;

    @Override
    public Integer call() {
        KafkaConsumer<String, String> consumer = createConsumer(broker);
        KafkaProducer<Long, Stats> producer = createProducer(broker);
        consumer.subscribe(Collections.singletonList(jsonlTopic));

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Exiting...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Map<Long, TreeSet<String>> uniqUsers = new TreeMap<>();
        try {
            long timePrev = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> recordCollection = consumer.poll(100);
                processRecords(recordCollection, uniqUsers);
                long timeCurr = System.currentTimeMillis();
                long secondDiff = (timeCurr - timePrev) / 1000;
                if (secondDiff >= reportPeriodSec) {
                    sendUniqUsers(producer, uniqUsers);
                    printStats(uniqUsers);
                    timePrev = timeCurr;
                }
            }
        } catch (WakeupException e) {

        } finally {
            consumer.close();
        }
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new KafkaApp()).execute(args);
        System.exit(exitCode);
    }

    public KafkaProducer<Long, Stats> createProducer(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "IdCounter");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StatsSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public KafkaConsumer<String, String> createConsumer(String broker) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "IdCounter");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public void sendUniqUsers(KafkaProducer<Long, Stats> producer, Map<Long, TreeSet<String>> uniqUsers) {
        Map<Long, Integer> minuteToCount = new TreeMap<>();
        for (Long key : uniqUsers.keySet()) {
            minuteToCount.put(key, uniqUsers.get(key).size());
        }
        Stats stats = new Stats(minuteToCount);

        ProducerRecord<Long, Stats> record = new ProducerRecord<>(statsTopic, stats);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {

        } catch (ExecutionException e) {

        }
    }

    public void processRecords(ConsumerRecords<String, String> recordCollection, Map<Long, TreeSet<String>> uniqUsers) {
        ObjectMapper jsonMapper = new ObjectMapper();
        for (ConsumerRecord<String, String> record : recordCollection) {
            String jsonString = record.value();
            JsonNode json = null;
            try {
                json = jsonMapper.readValue(jsonString, JsonNode.class);
            } catch (JsonParseException e) {

            } catch (JsonMappingException e) {

            } catch (java.io.IOException e) {

            }
            String strUid = json.get("uid").toString();
            long ts = json.get("ts").asLong();
            long tsFlooredToMinute = (ts / 60) * 60;
            if (!uniqUsers.containsKey(tsFlooredToMinute)) {
                uniqUsers.put(tsFlooredToMinute, new TreeSet<>());
            }
            uniqUsers.get(tsFlooredToMinute).add(strUid);
        }
    }

    public void printStats(Map<Long, TreeSet<String>> uniqUsers) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        System.out.println(dateFormatter.format(LocalDateTime.now()));
        System.out.println("Unique users structure:");
        System.out.println("-----------------------");
        System.out.printf("| %s | %s |\n", "Minute", "Unique users");
        for (Long minute : uniqUsers.keySet()) {
            Date time = new Date(minute * 1000);
            System.out.println("|" + time.toString() + " | " + uniqUsers.get(minute).size() + "|");
        }
        System.out.println();
    }
}
