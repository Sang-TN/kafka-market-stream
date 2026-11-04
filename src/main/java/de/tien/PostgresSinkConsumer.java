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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.core.JacksonException;
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

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("key.serializer", StringSerializer.class.getName());
        producerConfig.put("value.serializer", StringSerializer.class.getName());
        String sql = "INSERT INTO prices (ticker, event_time, price) VALUES (?, ?, ?)";

        try (
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
                Connection connection = DriverManager.getConnection(url, user, password);
                PreparedStatement statement = connection.prepareStatement(sql);
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {

            consumer.subscribe(List.of("prices"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String json = record.value();
                    PriceEvent priceEvent;

                    try {
                        priceEvent = mapper.readValue(json, PriceEvent.class);
                    } catch (JacksonException e) {
                        System.err.printf("Sent invalid event to DLQ (partition %d, offset %d): %s%n",
                                record.partition(),
                                record.offset(), e.getMessage());
                        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>("prices-dlq", record.key(),
                                json);
                        producer.send(dlqRecord);
                        continue;
                    }

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
