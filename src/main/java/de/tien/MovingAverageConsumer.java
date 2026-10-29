package de.tien;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.List;
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
        }
    }
}
