package com.example.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamingTablesv1 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("SHOW CATALOGS").print();

        tableEnv.executeSql("SHOW DATABASES").print();

        tableEnv.executeSql("SHOW FUNCTIONS").print();

        tableEnv.executeSql("SHOW TABLES").print();
    }
}

