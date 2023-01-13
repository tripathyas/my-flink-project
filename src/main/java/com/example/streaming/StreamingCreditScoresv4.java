package com.example.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.concat;

public class StreamingCreditScoresv4 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> recordsStream =
                env.readTextFile("src/main/resources/credit.csv");

        DataStream<CreditRecord> creditStream = recordsStream
            .filter((FilterFunction<String>) line -> !line.contains(
                "ID,LoanStatus,LoanAmount,Term,CreditScore,AnnualIncome,Home,CreditBalance"))
            .map(new MapFunction<String, com.example.streaming.CreditRecord>() {

                @Override
                public CreditRecord map(String s) throws Exception {

                    String[] fields = s.split(",");

                    return new CreditRecord(fields[0], fields[1], Double.parseDouble(fields[2]),
                                            fields[3], Double.parseDouble(fields[4]),
                                            Double.parseDouble(fields[5]),
                                            fields[6], Double.parseDouble(fields[7]));
                }
            });

        tableEnv.createTemporaryView("CreditDetails", creditStream);

        Table resultsTable = tableEnv.sqlQuery(
                "select id, creditScore, annualIncome, loanStatus from CreditDetails "+
                        " where annualIncome > 1500000 and creditScore < 700")
                .select($("id"), $("creditScore"), $("annualIncome"));

        TableResult result = resultsTable.execute();

        result.print();
    }
}

