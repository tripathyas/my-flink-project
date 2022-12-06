package com.example.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class StreamingStockPricesv3 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Tuple3<String, String, Double>> msft2020 =
                env.readTextFile("src/main/resources/MSFT_2020.csv")
                        .filter((FilterFunction<String>) line ->
                                !line.contains("Date,Open,High,Low,Close,Adj Close,Volume,Name"))
                        .map(new ExtractClosingPricesFn());

        DataStream<Tuple3<String, String, Double>> amzn2020 =
                env.readTextFile("src/main/resources/AMZN_2020.csv")
                        .filter((FilterFunction<String>) line ->
                                !line.contains("Date,Open,High,Low,Close,Adj Close,Volume,Name"))
                        .map(new ExtractClosingPricesFn());

        ConnectedStreams<Tuple3<String, String, Double>,
                         Tuple3<String, String, Double>> connectedStreams = msft2020.connect(amzn2020);

        connectedStreams.map(new CoMapFunction<Tuple3<String, String, Double>,
                                               Tuple3<String, String, Double>, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map1(Tuple3<String, String, Double> input)
                    throws Exception {
                return Tuple2.of(input.f0, Math.round(input.f2));
            }

            @Override
            public Tuple2<String, Long> map2(Tuple3<String, String, Double> input)
                    throws Exception {
                return Tuple2.of(input.f0, Math.round(input.f2));
            }
        }).print();

        env.execute();
    }

    public static class ExtractClosingPricesFn implements
            MapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> map(String s) throws Exception {
            String[] tokens = s.split(",");

            return new Tuple3<>(tokens[0], tokens[7], Double.parseDouble(tokens[5]));
        }
    }
}
