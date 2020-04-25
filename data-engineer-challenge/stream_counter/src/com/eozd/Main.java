package com.eozd;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "IdCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
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
        ObjectMapper jsonMapper = new ObjectMapper();
        try {
            long timePrev = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> recordCollection = consumer.poll(100);
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
                long timeCurr = System.currentTimeMillis();
                long secondDiff = (timeCurr - timePrev) / 1000;
                if (secondDiff >= 5) {
                    printStats(uniqUsers);
                    timePrev = timeCurr;
                }
            }
        } catch (WakeupException e) {

        } finally {
            consumer.close();
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
