package com.eozd;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Main {
    static final String BROKER = "localhost:9092";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer(BROKER);
        KafkaProducer<Long, Stats> producer = createProducer(BROKER);
        consumer.subscribe(Collections.singletonList("kafka_data"));

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

        Map<Long, TreeSet<String>> uniqUsers = new TreeMap<Long, TreeSet<String>>();
        try {
            long timePrev = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> recordCollection = consumer.poll(100);
                processRecords(recordCollection, uniqUsers);
                long timeCurr = System.currentTimeMillis();
                long secondDiff = (timeCurr - timePrev) / 1000;
                if (secondDiff >= 5) {
                    sendUniqUsers(producer, uniqUsers);
                    printStats(uniqUsers);
                    timePrev = timeCurr;
                }
            }
        } catch (WakeupException e) {

        } finally {
            consumer.close();
        }
    }

    public static KafkaProducer<Long, Stats> createProducer(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "IdCounter");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StatsSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> createConsumer(String broker) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "IdCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    public static void sendUniqUsers(KafkaProducer<Long, Stats> producer, Map<Long, TreeSet<String>> uniqUsers) {
        Map<Long, Integer> minuteToCount = new TreeMap<>();
        for (Long key : uniqUsers.keySet()) {
            minuteToCount.put(key, uniqUsers.get(key).size());
        }
        Stats stats = new Stats(minuteToCount);

        ProducerRecord<Long, Stats> record = new ProducerRecord<>("stats", stats);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {

        } catch (ExecutionException e) {

        }
    }

    public static void processRecords(ConsumerRecords<String, String> recordCollection, Map<Long, TreeSet<String>> uniqUsers) {
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

    public static void printStats(Map<Long, TreeSet<String>> uniqUsers) {
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
