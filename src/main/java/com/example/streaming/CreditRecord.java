package com.example.streaming;

public class CreditRecord {

    public String id;
    public String loanStatus;
    public Double loanAmount;
    public String term;
    public Double creditScore;
    public Double annualIncome;
    public String home;
    public Double creditBalance;

    public CreditRecord() {
    }

    public CreditRecord(String id, String loanStatus, Double loanAmount,
                        String term, Double creditScore, Double annualIncome,
                        String home, Double creditBalance) {
        this.id = id;
        this.loanStatus = loanStatus;
        this.loanAmount = loanAmount;
        this.term = term;
        this.creditScore = creditScore;
        this.annualIncome = annualIncome;
        this.home = home;
        this.creditBalance = creditBalance;
    }
}
