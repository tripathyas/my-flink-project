package com.example.streaming;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingCarsv2 {

    public static class Car {

        public String brand;
        public String model;
        public String year;
        public Integer price;
        public Integer miles;

        public Car() {
        }

        public Car(String brand, String model, String year, Integer price, Integer miles) {
            this.brand = brand;
            this.model = model;
            this.year = year;
            this.price = price;
            this.miles = miles;
        }

        @Override
        public String toString() {
            return brand + ", " + model + ", " + year + ", " + price + ", " + miles;
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> carStream =
                env.readTextFile("src/main/resources/USA_cars.csv");

        DataStream<String> filteredStream = carStream.filter(
                (FilterFunction<String>) line ->
                        !line.contains("brand,model,year,price,mileage"));

        DataStream<Car> carDetails = filteredStream.map(
                new RichMapFunction<String, Car>() {

                    private final Histogram priceHistogram = new Histogram();

                    public Car map(String row) throws Exception {
                        String[] fields = row.split(",");

                        Car car = new Car(fields[0], fields[1], fields[2],
                                Integer.parseInt(fields[3]), Integer.parseInt(fields[4]));

                        priceHistogram.add(car.price);

                        return car;
                    }

                    @Override
                    public void open(Configuration config) {
                        getRuntimeContext().addAccumulator("all_prices", this.priceHistogram);
                    }
                });

        DataStream<Car> filteredCarDetails = carDetails.filter(new RichFilterFunction<Car>() {

            private final Histogram priceHistogram = new Histogram();

            @Override
            public boolean filter(Car car) throws Exception {
                if (car.brand.equals("bmw")) {
                    priceHistogram.add(car.price);

                    return true;
                }

                return false;
            }

            @Override
            public void open(Configuration config) {
                getRuntimeContext().addAccumulator("bmw_prices", this.priceHistogram);
            }
        });

        filteredCarDetails.print();

        JobExecutionResult result = filteredCarDetails.getExecutionEnvironment().execute();

        System.out.println("BMW Prices: " + result.getAccumulatorResult("bmw_prices"));
        System.out.println("All car Prices: " + result.getAccumulatorResult("all_prices"));
    }
}
