package de.tien;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
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
    }
}