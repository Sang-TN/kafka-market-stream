package de.tien;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.List;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import tools.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MovingAverageConsumer {
    public static void main(String[] args) {
        HashMap<String, ArrayList<Double>> priceHistory = new HashMap<>();
        int windowSize = 10;

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", "moving-average");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());

        JsonMapper mapper = JsonMapper.builder().build();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of("prices"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String ticker = record.key();
                    String json = record.value();

                    PriceEvent priceEvent = mapper.readValue(json, PriceEvent.class);

                    double price = priceEvent.price();

                    ArrayList<Double> liste = priceHistory.get(ticker);
                    if (liste == null) {
                        liste = new ArrayList<>();
                        priceHistory.put(ticker, liste);
                    }
                    liste.add(price);

                    if (liste.size() > windowSize) {
                        liste.remove(0);
                    }
                    double sum = 0;
                    for (double p : liste) {
                        sum += p;
                    }
                    double movingAverage = (double) sum / liste.size();
                    System.out.printf("Ticker: %s, Partition: %d, Offset: %d, Moving Average: %.2f%n", ticker,
                            record.partition(), record.offset(), movingAverage);
                }
            }
        }
    }
}
