package com.example.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class StreamingGameScoresv2 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9000);

        DataStream<String> gameScores = dataStream
                .map(new ExtractPlayersAndScoresFn())
                .filter(new FilterPlayersAboveThresholdFn(100))
                .map(new ConvertToStringFn());

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("src/main/resources/streamingSink"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .build();

        gameScores.addSink(sink);
        gameScores.print();

        env.execute("Streaming game scores");
    }

    public static class ExtractPlayersAndScoresFn implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            String[] tokens = s.split(",");

            return Tuple2.of(tokens[0].trim(), Integer.parseInt(tokens[1].trim()));
        }
    }

    public static class FilterPlayersAboveThresholdFn implements FilterFunction<Tuple2<String, Integer>> {

        private int scoreThreshold = 0;

        public FilterPlayersAboveThresholdFn(int scoreThreshold) {
            this.scoreThreshold = scoreThreshold;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> playersScores) throws Exception {
            return playersScores.f1 > scoreThreshold;
        }
    }

    public static class ConvertToStringFn implements MapFunction<Tuple2<String, Integer>, String> {

        @Override
        public String map(Tuple2<String, Integer> playersScores) throws Exception {
            return playersScores.f0 + ", " + playersScores.f1;
        }
    }

}
