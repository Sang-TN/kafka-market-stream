package de.tien;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import java.beans.Transient;
import java.util.HashMap;

public class PortfolioValueConsumerTest {
    @Test
    public void valueOfTwoStocks() {

        HashMap<String, Double> stockPrices = new HashMap<>();
        HashMap<String, Integer> stockQuantities = new HashMap<>();
        stockPrices.put("NVDA", 100.0);
        stockQuantities.put("NVDA", 2);
        stockPrices.put("SAP", 50.0);
        stockQuantities.put("SAP", 3);

        double totalValue = PortfolioValueConsumer.calculatePortfolioValue(stockQuantities, stockPrices);

        assertEquals(350.0, totalValue, 0.0001);
    }

    @Test
    public void valueOfSingleStock() {

        HashMap<String, Double> stockPrices = new HashMap<>();
        HashMap<String, Integer> stockQuantities = new HashMap<>();
        stockPrices.put("NVDA", 360.0);
        stockQuantities.put("NVDA", 4);

        double totalValue = PortfolioValueConsumer.calculatePortfolioValue(stockQuantities, stockPrices);

        assertEquals(1440.0, totalValue, 0.0001);
    }
}