package com.pluralsight.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;

public class StreamingLifeExpectancies {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final Schema schema = new Schema()
                .field("Country", DataTypes.STRING())
                .field("Year", DataTypes.INT())
                .field("Status", DataTypes.STRING())
                .field("Life Expectancy", DataTypes.DOUBLE());

        tableEnv.connect(new FileSystem().path("src/main/resources/life_exp.csv"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("LifeExpectancyTable");

        Table table = tableEnv.sqlQuery("SELECT Country, Status, `Life Expectancy`  " +
                                            "FROM LifeExpectancyTable " +
                                            "WHERE `Life Expectancy` >= 80.0");

        CsvTableSink sink = new CsvTableSink(
                "src/main/resources/life_exp.csv","|",1,
                org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

        tableEnv.registerTableSink(
                "HighLifeExpectancyTable",
                new String[]{"Country", "Status", "Life Expectancy"},
                new TypeInformation[]{ Types.STRING, Types.STRING,Types.DOUBLE },
                sink);

        TableResult tableResult = table.execute();

        table.executeInsert("HighLifeExpectancyTable");

        tableEnv.executeSql("SHOW TABLES").print();
    }
}