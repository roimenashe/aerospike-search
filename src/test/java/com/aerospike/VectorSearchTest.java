package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.model.IndexType;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class VectorSearchTest {

    private static IAerospikeClient aerospikeClient;
    private static final String NAMESPACE = "test";
    private static final String SET = "docs";

    @BeforeAll
    static void setupAerospike() {
        aerospikeClient = new AerospikeClient("localhost", 3000);

        // Clear previous records
        aerospikeClient.truncate(null, NAMESPACE, SET, null);

        WritePolicy wp = new WritePolicy();

        // Insert a few sample records with short text fields
        aerospikeClient.put(wp, new Key(NAMESPACE, SET, "doc1"),
                new Bin("title", "Lucene in Action"),
                new Bin("body", "Full text search library for Java."));

        aerospikeClient.put(wp, new Key(NAMESPACE, SET, "doc2"),
                new Bin("title", "Aerospike and Lucene"),
                new Bin("body", "Combining Aerospike speed with Lucene indexing."));

        aerospikeClient.put(wp, new Key(NAMESPACE, SET, "doc3"),
                new Bin("title", "Distributed Databases"),
                new Bin("body", "Aerospike provides low latency storage."));
    }

    @AfterAll
    static void tearDown() {
        aerospikeClient.truncate(null, NAMESPACE, SET, null);
        aerospikeClient.close();
    }

    @Test
    void testVectorIndexAndSearch() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Build vector index from Aerospike data
            search.createVectorIndex(NAMESPACE, SET, getEmbedder());

            // Query vector representing "Lucene search"
            float[] queryVector = new float[]{1f, 0f, 1f};

            // Perform vector search (top 2 results)
            List<Record> results = search.searchVector(NAMESPACE, SET, queryVector, 2);

            results.forEach(System.out::println);
            Assertions.assertEquals(2, results.size());

            // Verify that most relevant results include Lucene-related docs
            boolean hasLuceneMatch = results.stream()
                    .anyMatch(r -> {
                        String title = r.getString("title").toLowerCase();
                        return title.contains("lucene");
                    });

            Assertions.assertTrue(hasLuceneMatch, "Expected Lucene-related record in results");
        }
    }

    @Test
    void testTooLargeK() throws Exception {
        AerospikeSearch search = new AerospikeSearch(aerospikeClient);

        search.createVectorIndex(NAMESPACE, SET, getEmbedder());

        // Large K should throw an exception
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> search.searchVector(NAMESPACE, SET, new float[]{1f, 0f, 1f}, 1000));
    }

    // Simple deterministic embedding generator
    private Function<Record, float[]> getEmbedder() {
        return record -> {
            String text = record.getString("title") + " " + record.getString("body");
            text = text.toLowerCase();

            float lucene = text.contains("lucene") ? 1f : 0f;
            float aerospike = text.contains("aerospike") ? 1f : 0f;
            float searchWord = text.contains("search") ? 1f : 0f;

            // 3D toy vector embedding
            return new float[]{lucene, aerospike, searchWord};
        };
    }

    @Test
    void testListVectorIndexes() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            search.createVectorIndex(NAMESPACE, SET, getEmbedder());
            search.createVectorIndex(NAMESPACE, "set2", getEmbedder());
            search.createVectorIndex(NAMESPACE, "set3", getEmbedder());

            Map<String, IndexType> indexes = search.listIndexes();

            // Exactly 3 full-text indexes
            Assertions.assertEquals(3, indexes.size());
            indexes.forEach((s, indexType) -> Assertions.assertEquals(IndexType.VECTOR, indexType));
        }
    }
}
