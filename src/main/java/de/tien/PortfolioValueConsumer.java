package de.tien;

import java.util.HashMap;
import java.util.Properties;
import java.time.Duration;
import java.util.List;
import tools.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PortfolioValueConsumer {
    public static void main(String[] args) {
        HashMap<String, Integer> holdings = new HashMap<>();
        HashMap<String, Double> lastPrices = new HashMap<>();

        holdings.put("NVDA", 10);
        holdings.put("SAP", 20);
        holdings.put("TSLA", 5);

        JsonMapper mapper = JsonMapper.builder().build();
        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", "portfolio-value");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of("prices"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record : records) {
                    String ticker = record.key();
                    String json = record.value();

                    PriceEvent priceEvent = mapper.readValue(json, PriceEvent.class);
                    double price = priceEvent.price();
                    lastPrices.put(ticker, price);

                    boolean allPricesAvailable = lastPrices.size() == holdings.size();
                    if (allPricesAvailable) {
                        double totalValue = 0.0;
                        for (String stock : holdings.keySet()) {
                            int quantity = holdings.get(stock);
                            double lastPrice = lastPrices.get(stock);
                            totalValue += quantity * lastPrice;
                        }
                        System.out.printf("%-6s %.2f  →  Portfolio: %.2f%n", ticker, price, totalValue);
                    } 
                }
            } 
        }  

    }
}