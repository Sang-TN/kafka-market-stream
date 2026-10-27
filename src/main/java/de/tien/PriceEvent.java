package de.tien;
import java.time.Instant;

public record PriceEvent(String ticker, Instant timestamp, double price){

}
