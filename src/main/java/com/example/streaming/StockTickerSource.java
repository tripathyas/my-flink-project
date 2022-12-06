package com.pluralsight.streaming;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Instant;
import java.util.*;

public class StockTickerSource extends RichParallelSourceFunction<StockPrices> {

    private boolean running = true;

    private static final Map<String, Double> stockTickersMap = new HashMap<>();

    static {
        stockTickersMap.put("MSFT", 200.0);
        stockTickersMap.put("AMZN", 3000.0);
        stockTickersMap.put("FB", 300.0);
        stockTickersMap.put("GOOG", 1500.0);
    }

    @Override
    public void run(SourceContext<StockPrices> srcCtx) throws Exception {

        Random rand = new Random();

        while (running) {

            long currTime = Instant.now().toEpochMilli();

            for (Map.Entry<String, Double> entry : stockTickersMap.entrySet()) {

                srcCtx.collect(new StockPrices(
                        entry.getKey(),
                        entry.getValue() + (rand.nextGaussian() * 20),
                        currTime));
            }

            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}