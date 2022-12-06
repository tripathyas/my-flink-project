package com.pluralsight.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class StreamingStockTickers {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<StockPrices> stockStream = env
                .addSource(new StockTickerSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<StockPrices>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp));

        Table stockTable = tableEnv.fromDataStream(stockStream,
                $("ticker"), $("price"), $("timestamp").rowtime());

        Table windowedTable = stockTable
                .window(Tumble.over(lit(10).seconds())
                        .on($("timestamp"))
                        .as("stockPriceWindow"))
                .groupBy($("ticker"), $("stockPriceWindow"))
                .select($("ticker"), $("price").max(),
                        $("stockPriceWindow").start(), $("stockPriceWindow").end(),
                        $("stockPriceWindow").rowtime());

        DataStream<Tuple2<Boolean, Row>> outputStream =
                tableEnv.toRetractStream(windowedTable, Row.class);

        outputStream.print();

        env.execute();
    }

}
