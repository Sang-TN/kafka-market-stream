package de.tien;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class MovingAverageConsumerTest {
    @Test
    public void averageOfThreePrices() {

        ArrayList<Double> prices = new ArrayList<>();
        prices.add(10.0);
        prices.add(20.0);
        prices.add(60.0);

        double average = MovingAverageConsumer.calculateAverage(prices);

        assertEquals(30.0, average, 0.0001);
    }

}
