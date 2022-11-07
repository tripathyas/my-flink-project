package com.example.streaming;

public class Movie {

    private String movieTitle;
    private String directorName;
    private String genre;
    private Long gross;

    public Movie() {
    }

    public Movie(String movieTitle, String directorName, String genre, Long gross) {
        this.movieTitle = movieTitle;
        this.directorName = directorName;
        this.genre = genre;
        this.gross = gross;
    }

    public String getMovieTitle() {
        return movieTitle;
    }

    public void setMovieTitle(String movieTitle) {
        this.movieTitle = movieTitle;
    }

    public String getDirectorName() {
        return directorName;
    }

    public void setDirectorName(String directorName) {
        this.directorName = directorName;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public Long getGross() {
        return gross;
    }

    public void setGross(Long gross) {
        this.gross = gross;
    }

    @Override
    public String toString() {
        return movieTitle + "," + directorName + "," + genre + "," + gross;
    }
}


