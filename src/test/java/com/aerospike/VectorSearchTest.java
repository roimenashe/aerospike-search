package com.aerospike;

import com.aerospike.client.Record;
import com.aerospike.model.IndexType;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class VectorSearchTest extends BaseTest {

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
