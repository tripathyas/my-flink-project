package com.example.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;

public class MovieSelection {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple8<String, String, Integer, Integer, String, Long,
                       String, String>> records =
                env.readCsvFile("src/main/resources/movies.csv")
                        .ignoreFirstLine()
                        .parseQuotedStrings('"')
                        .ignoreInvalidLines()
                        .types(String.class, String.class, Integer.class, Integer.class,
                               String.class, Long.class, String.class, String.class);

        records.filter((FilterFunction<Tuple8<String, String, Integer, Integer, String, Long, String, String>>)
                movie -> movie.f1.contains("James Cameron"))
                    .print();
    }

}
