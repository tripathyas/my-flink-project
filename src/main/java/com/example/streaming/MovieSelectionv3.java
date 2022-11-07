package com.example.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple8;

public class MovieSelectionv3 {

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

        DataSet<Movie> filteredMovies = records.map(new ConvertToMovieObjects())
                .filter(new FilterGenre("Romance"));

        filteredMovies.print();
    }

    public static class ConvertToMovieObjects implements MapFunction<
            Tuple8<String, String, Integer, Integer, String, Long, String, String>, Movie> {

        @Override
        public Movie map(Tuple8<String, String, Integer, Integer, String, Long, String, String> row)
                throws Exception {

            return new Movie(row.f6, row.f1, row.f7, row.f5);
        }
    }

    public static class FilterGenre implements FilterFunction<Movie> {

        private String genre;

        public FilterGenre(String genre) {
            this.genre = genre;
        }

        @Override
        public boolean filter(Movie movie) throws Exception {
            System.out.println(movie.getMovieTitle() + " " + movie.getGenre());

            return movie.getGenre().contains(genre);
        }
    }

}
