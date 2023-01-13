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

public class StreamingCreditScoresv3 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> recordsStream =
                env.readTextFile("src/main/resources/credit.csv");

        DataStream<Tuple8<String, String, Double, String, Double, Double, String, Double>> creditStream = recordsStream
            .filter((FilterFunction<String>) line -> !line.contains(
                "ID,LoanStatus,LoanAmount,Term,CreditScore,AnnualIncome,Home,CreditBalance"))
            .map(new MapFunction<String, Tuple8<String, String, Double, String, Double, Double, String, Double>>() {

                @Override
                public Tuple8<String, String, Double, String, Double, Double, String, Double> map(String s)
                        throws Exception {

                    String[] fields = s.split(",");

                    return Tuple8.of(fields[0], fields[1], Double.parseDouble(fields[2]),
                                     fields[3], Double.parseDouble(fields[4]),
                                     Double.parseDouble(fields[5]),
                                     fields[6], Double.parseDouble(fields[7]));
                }
            });

        tableEnv.createTemporaryView("CreditDetails", creditStream,
                $("ID"), $("LoanStatus"), $("LoanAmount"),
                $("Term"), $("CreditScore"), $("AnnualIncome"),
                $("Home"), $("CreditBalance"));

        Table creditDetailsTable = tableEnv.from("CreditDetails");

        Table resultsTable = creditDetailsTable.select($("ID"), $("LoanAmount"), $("AnnualIncome"));

        TableResult result = resultsTable.execute();

        result.print();
    }
}

