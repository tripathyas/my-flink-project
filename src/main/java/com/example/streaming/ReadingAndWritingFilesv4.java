package com.example.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.core.fs.Path;


public class ReadingAndWritingFilesv4 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        String path = "src/main/resources/rioOlympics.txt";
        String outputPath = "src/main/resources/streamingSink";

        TextInputFormat format = new TextInputFormat(new Path(path));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        DataStream<String> inputStream = env.readFile(format, path,
                FileProcessingMode.PROCESS_ONCE, 30000);

        DataStream<String> headerStream = inputStream.filter(
                (FilterFunction<String>) input -> input.startsWith("Rio"));

        DataStream<String> filterData = inputStream.filter(
                (FilterFunction<String>) input -> input.contains("Gold:0"));

        DataStream<String> finalStream = headerStream.union(filterData);

        finalStream.print();

        env.execute("Processing File Continuously");
    }
}
