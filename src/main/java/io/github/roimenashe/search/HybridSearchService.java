package io.github.roimenashe.search;

import io.github.roimenashe.model.ScoredId;
import io.github.roimenashe.model.SimilarityFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HybridSearchService {

    private final FullTextSearchService fullTextSearchService;
    private final VectorSearchService vectorService;

    public HybridSearchService(FullTextSearchService fullTextSearchService, VectorSearchService vectorService) {
        this.fullTextSearchService = fullTextSearchService;
        this.vectorService = vectorService;
    }

    public List<String> searchHybrid(String namespace,
                                     String set,
                                     String textQuery,
                                     float[] queryVector,
                                     SimilarityFunction similarityFunction,
                                     int limit,
                                     double textWeight,
                                     double vectorWeight) throws Exception {
        validateWeights(textWeight, vectorWeight);

        List<ScoredId> textResults = fullTextSearchService.searchWithScores(namespace, set, textQuery, limit);
        List<ScoredId> vectorResults = vectorService.searchWithScores(namespace, set, queryVector, limit, similarityFunction);

        normalizeScores(textResults);
        normalizeScores(vectorResults);

        Map<String, Double> combined = new HashMap<>();
        for (ScoredId r : textResults) combined.put(r.id, r.score * textWeight);
        for (ScoredId r : vectorResults)
            combined.merge(r.id, r.score * vectorWeight, Double::sum);

        return combined.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private void validateWeights(double textWeight, double vectorWeight) {
        if (textWeight < 0 || vectorWeight < 0) {
            throw new IllegalArgumentException("Weights must be non-negative");
        }
        if (Math.abs(textWeight + vectorWeight - 1.0) > 0.001) {
            throw new IllegalArgumentException("Weights must sum to 1.0");
        }
    }

    private void normalizeScores(List<ScoredId> results) {
        if (results.isEmpty()) return;
        double max = results.stream().mapToDouble(r -> r.score).max().orElse(1.0);
        for (ScoredId r : results) r.score /= max;
    }
}
