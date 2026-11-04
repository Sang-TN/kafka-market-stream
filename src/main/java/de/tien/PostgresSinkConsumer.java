package de.tien;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Properties;
import java.time.Duration;
import tools.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.List;

public class PostgresSinkConsumer {
    public static void main(String[] args) throws SQLException {

        String url = "jdbc:postgresql://localhost:5432/marketdata";
        String user = "market";
        String password = "market";

        JsonMapper mapper = JsonMapper.builder().build();
        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", "postgres-sink");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());

        String sql = "INSERT INTO prices (ticker, event_time, price) VALUES (?, ?, ?)";

        try (
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
                Connection connection = DriverManager.getConnection(url, user, password);
                PreparedStatement statement = connection.prepareStatement(sql)) {

            consumer.subscribe(List.of("prices"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String json = record.value();
                    PriceEvent priceEvent = mapper.readValue(json, PriceEvent.class);
                    statement.setString(1, priceEvent.ticker());
                    statement.setTimestamp(2, Timestamp.from(priceEvent.timestamp()));
                    statement.setDouble(3, priceEvent.price());
                    statement.executeUpdate();
                    System.out
                            .println(
                                    "Saved " + priceEvent.ticker() + " price to database: " + priceEvent.price());
                }
            }
        }

    }

}
