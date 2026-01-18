package io.github.roimenashe.model;

public class ScoredId {
    private final String id;
    private double score;

    public ScoredId(String id, double score) {
        this.id = id;
        this.score = score;
    }

    public String getId() {
        return id;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}

