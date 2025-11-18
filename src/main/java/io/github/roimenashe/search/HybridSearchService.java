package io.github.roimenashe.search;

import io.github.roimenashe.model.SimilarityFunction;

import java.util.*;
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
        // Run both searches
        List<ScoredId> textResults = fullTextSearchService.searchWithScores(namespace, set, textQuery, limit);
        List<ScoredId> vectorResults = vectorService.searchWithScores(namespace, set, queryVector, limit, similarityFunction);

        // Normalize each list’s scores to [0,1]
        normalizeScores(textResults);
        normalizeScores(vectorResults);

        // Merge maps
        Map<String, Double> combined = new HashMap<>();
        for (ScoredId r : textResults) combined.put(r.id, r.score * textWeight);
        for (ScoredId r : vectorResults)
            combined.merge(r.id, r.score * vectorWeight, Double::sum);

        // Sort by combined score
        return combined.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private void normalizeScores(List<ScoredId> results) {
        if (results.isEmpty()) return;
        double max = results.stream().mapToDouble(r -> r.score).max().orElse(1.0);
        for (ScoredId r : results) r.score /= max;
    }

    public static class ScoredId {
        public final String id;
        public double score;

        public ScoredId(String id, double score) {
            this.id = id;
            this.score = score;
        }
    }
}
