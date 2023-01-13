package com.example.streaming;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class StockPrices {

    public String ticker;
    public Double price;
    public long timestamp;

    public StockPrices() {
    }

    public StockPrices(String ticker, double price, long timestamp) {
        this.ticker = ticker;
        this.price = price;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
                this.timestamp/1000, 0, ZoneOffset.UTC);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        String formattedDate = dateTime.format(formatter);

        return "(" + this.ticker + ", " + this.price + ", " + formattedDate + ")";
    }
}
