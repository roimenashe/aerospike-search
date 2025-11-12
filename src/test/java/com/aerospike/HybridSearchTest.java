package com.aerospike;

import com.aerospike.client.Record;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class HybridSearchTest extends BaseTest {

    @Test
    void testHybridSearchWithVectorBin() throws Exception {
        String vectorBin = "vectorBin";

        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Create full-text and vector indexes
            search.createFullTextIndex(NAMESPACE, SET);
            search.createVectorIndex(NAMESPACE, SET, vectorBin);

            // Query: "Lucene" text + semantic vector [1,0,1]
            float[] queryVector = new float[]{1f, 0f, 1f};
            List<Record> results = search.searchHybrid(NAMESPACE, SET, "Lucene", queryVector, 10, 0.6, 0.4);

            results.forEach(System.out::println);

            // Expect all 3 records (union of text and vector matches)
            Assertions.assertEquals(3, results.size());

            // Ensure Lucene-related docs appear
            boolean hasLuceneMatch = results.stream()
                    .anyMatch(r -> r.getString("title").toLowerCase().contains("lucene"));
            Assertions.assertTrue(hasLuceneMatch, "Expected Lucene-related records in hybrid results");
        }
    }

    @Test
    void testHybridSearchWithEmbeddingFunction() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            search.createFullTextIndex(NAMESPACE, SET);

            // Build a simple vector index using a toy embedder
            search.createVectorIndex(NAMESPACE, SET, record -> {
                String text = (record.getString("title") + " " + record.getString("body")).toLowerCase();
                float lucene = text.contains("lucene") ? 1f : 0f;
                float aerospike = text.contains("aerospike") ? 1f : 0f;
                float searchWord = text.contains("search") ? 1f : 0f;
                return new float[]{lucene, aerospike, searchWord};
            });

            // Run hybrid search combining text and vector scores
            float[] queryVector = new float[]{1f, 0f, 1f}; // "Lucene search"
            List<Record> results = search.searchHybrid(NAMESPACE, SET, "Lucene", queryVector, 10, 0.6, 0.4);

            results.forEach(System.out::println);

            // Expect all 3 records (union of text + vector matches)
            Assertions.assertEquals(3, results.size());
        }
    }
}
