package com.eozd;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


@Command(name = "kafka_app", mixinStandardHelpOptions = true, version = "0.1", description = "Reads frames streamed in .jsonl format and produces unique user" + " counts for each minute in the data. The results are written both to STDOUT" + " and to a new Kafka topic.")
class KafkaApp implements Callable<Integer> {
    @Option(names="--broker", defaultValue = "localhost:9092", description = "Kafka server address (default: ${DEFAULT-VALUE})")
    private String broker = "localhost:9092";

    @Option(names="--report_period_sec", defaultValue = "5", description = "The time between two reports in seconds. This is used both for STDOUT" + " and for writing back to Kafka (default: ${DEFAULT-VALUE})")
    private int reportPeriodSec = 5;

    @Option(names="--jsonl_topic", defaultValue = "kafka_data", description = "Topic name of the frame data (default: ${DEFAULT-VALUE})")
    private String jsonlTopic = "kafka_data";

    @Option(names="--stats_topic", defaultValue = "stats", description = "Topic name of the output statistics (default: ${DEFAULT-VALUE})")
    private String statsTopic = "stats";

    @Option(names="--benchmark", description = "Print benchmark stats when process is terminated with Ctrl-C")
    private boolean benchmark = false;

    @Option(names="--benchmark_period_sec", defaultValue = "1", description = "Time between benchmark measurements in seconds (default: ${DEFAULT-VALUE})")
    private int benchmarkPeriodSec = 1;

    public static void main(String... args) {
        int exitCode = new CommandLine(new KafkaApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        KafkaConsumer<String, String> consumer = createConsumer(broker);
        KafkaProducer<Long, Stats> producer = createProducer(broker);
        consumer.subscribe(Collections.singletonList(jsonlTopic));
        setUpInterruptHook(consumer);

        Map<Long, TreeSet<String>> uniqUsers = new TreeMap<>();
        DescriptiveStatistics framesPerSec = new DescriptiveStatistics();
        try {
            mainLoop(consumer, producer, uniqUsers, framesPerSec);
        } catch (WakeupException e) {
            if (benchmark) {
                printBenchmarkResults(framesPerSec);
            }
        } finally {
            consumer.close();
        }
        return 0;
    }

    public void mainLoop(KafkaConsumer<String, String> consumer, KafkaProducer<Long, Stats> producer, Map<Long, TreeSet<String>> uniqUsers, DescriptiveStatistics framesPerSec) {
        long reportTimePrev = System.currentTimeMillis();
        long benchTimePrev = System.currentTimeMillis();
        long numFrames = 0;
        while (true) {
            ConsumerRecords<String, String> recordCollection = consumer.poll(100);
            numFrames += recordCollection.count();
            try {
                processRecords(recordCollection, uniqUsers);
            } catch (IllegalJSONException e) {
                e.printStackTrace();
            }

            long timeCurr = System.currentTimeMillis();
            long reportSecondDiff = (timeCurr - reportTimePrev) / 1000;
            if (reportSecondDiff >= reportPeriodSec) {
                sendUniqUsers(producer, uniqUsers);
            }

            if (benchmark) {
                timeCurr = System.currentTimeMillis();
                if (numFrames == 0) {
                    benchTimePrev = timeCurr;
                } else {
                    double benchSecondDiff = (timeCurr - benchTimePrev) / 1000.0;
                    if (benchSecondDiff >= benchmarkPeriodSec) {
                        double fps = numFrames / benchSecondDiff;
                        framesPerSec.addValue(fps);
                        benchTimePrev = timeCurr;
                        numFrames = 0;
                    }
                }
            }

            if (reportSecondDiff >= reportPeriodSec) {
                printStats(uniqUsers);
                reportTimePrev = timeCurr;
            }
        }
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

    public void processRecords(ConsumerRecords<String, String> recordCollection, Map<Long, TreeSet<String>> uniqUsers) throws IllegalJSONException {
        for (ConsumerRecord<String, String> record : recordCollection) {
            String jsonString = record.value();

            String uidKey = "\"uid\":";
            int uidKeyBegIdx = findOutermostKey(jsonString, uidKey);
            if (uidKeyBegIdx == -1) {
                throw new IllegalJSONException("JSON string must have \"uid\" as a top-level field.");
            }
            int uidValBegIdx = uidKeyBegIdx + uidKey.length() + 1;
            int uidValEndIdx = jsonString.indexOf('"', uidValBegIdx);
            String strUid = jsonString.substring(uidValBegIdx, uidValEndIdx);

            String tsKey = "\"ts\":";
            int tsKeyBegIdx = findOutermostKey(jsonString, tsKey);
            if (tsKeyBegIdx == -1) {
                throw new IllegalJSONException("JSON string must have \"ts\" as a top-level field.");
            }
            int tsValBegIdx = tsKeyBegIdx + tsKey.length();
            int tsValEndIdx = jsonString.indexOf(',', tsValBegIdx);
            String strTs = jsonString.substring(tsValBegIdx, tsValEndIdx);
            long ts = Long.parseLong(strTs);

            long tsFlooredToMinute = (ts / 60) * 60;
            if (!uniqUsers.containsKey(tsFlooredToMinute)) {
                uniqUsers.put(tsFlooredToMinute, new TreeSet<>());
            }
            uniqUsers.get(tsFlooredToMinute).add(strUid);
        }
    }

    public int findOutermostKey(String jsonString, String key) {
        int nCurlyBrackets = 0;
        for (int i = 0; i < jsonString.length() - key.length(); ++i) {
            if (jsonString.charAt(i) == '{') {
                nCurlyBrackets++;
            } else if (jsonString.charAt(i) == '}') {
                nCurlyBrackets--;
            }
            if (nCurlyBrackets != 1) continue;
            boolean equal = true;
            for (int j = 0; j < key.length(); ++j) {
                if (jsonString.charAt(i + j) != key.charAt(j)) {
                    equal = false;
                    break;
                }
            }
            if (equal) {
                return i;
            }
        }
        return -1;
    }

    public void printBenchmarkResults(DescriptiveStatistics framesPerSec) {
        System.out.println("BENCHMARK RESULTS");
        System.out.println("-----------------");

        double mean = framesPerSec.getMean();
        double std = framesPerSec.getStandardDeviation();
        double perc_5 = framesPerSec.getPercentile(5.0);
        double perc_50 = framesPerSec.getPercentile(50.0);
        double perc_95 = framesPerSec.getPercentile(95.0);
        System.out.println("FRAMES PER SECOND");
        System.out.println("Mean            : " + mean);
        System.out.println("Std             : " + std);
        System.out.println("Median          : " + perc_50);
        System.out.println("5th percentile  : " + perc_5);
        System.out.println("95th percentile : " + perc_95);

        for (double fps : framesPerSec.getValues()) {
            System.out.println(fps);
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

    public void setUpInterruptHook(KafkaConsumer<?, ?> consumer) {
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
    }
}
