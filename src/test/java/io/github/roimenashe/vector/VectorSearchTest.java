package io.github.roimenashe.vector;

import io.github.roimenashe.AerospikeSearch;
import io.github.roimenashe.BaseTest;
import com.aerospike.client.Record;
import io.github.roimenashe.model.IndexType;
import io.github.roimenashe.model.SimilarityFunction;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class VectorSearchTest extends BaseTest {

    @Test
    void testVectorIndexFromBinAndSearch() throws Exception {
        String vectorBin = "vectorBin";

        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Build vector index directly from Aerospike bin
            search.createVectorIndex(NAMESPACE, SET, vectorBin, SimilarityFunction.DOT_PRODUCT);

            // Query vector representing "Lucene search"
            float[] queryVector = new float[]{1f, 0f, 1f};

            // Perform vector search (top 2 results)
            List<Record> results = search.searchVector(NAMESPACE, SET, queryVector, 2, SimilarityFunction.DOT_PRODUCT);

            results.forEach(System.out::println);

            // Expect top 2 Lucene-related results
            Assertions.assertEquals(2, results.size());

            boolean hasLuceneMatch = results.stream()
                    .anyMatch(r -> {
                        String title = r.getString("title").toLowerCase();
                        return title.contains("lucene");
                    });

            Assertions.assertTrue(hasLuceneMatch, "Expected Lucene-related record in results");
        }
    }

    @Test
    void testVectorIndexWithEmbeddingFunctionAndSearch() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Build vector index from Aerospike data
            search.createVectorIndex(NAMESPACE, SET, getEmbedder(), SimilarityFunction.DOT_PRODUCT);

            // Query vector representing "Lucene search"
            float[] queryVector = new float[]{1f, 0f, 1f};

            // Perform vector search (top 2 results)
            List<Record> results = search.searchVector(NAMESPACE, SET, queryVector, 2, SimilarityFunction.DOT_PRODUCT);

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
    void testTooLargeK() throws Exception {
        AerospikeSearch search = new AerospikeSearch(aerospikeClient);

        search.createVectorIndex(NAMESPACE, SET, "vectorBin", SimilarityFunction.DOT_PRODUCT);

        // Large K should throw an exception
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> search.searchVector(NAMESPACE, SET, new float[]{1f, 0f, 1f}, 1000, SimilarityFunction.DOT_PRODUCT));
    }

    @Test
    void testListVectorIndexes() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            search.createVectorIndex(NAMESPACE, SET, "vectorBin", SimilarityFunction.DOT_PRODUCT);
            search.createVectorIndex(NAMESPACE, "set2", "vectorBin2", SimilarityFunction.DOT_PRODUCT);
            search.createVectorIndex(NAMESPACE, "set3", "vectorBin3", SimilarityFunction.DOT_PRODUCT);

            Map<String, IndexType> indexes = search.listIndexes();

            Assertions.assertEquals(3, indexes.size());
            indexes.forEach((s, indexType) -> Assertions.assertEquals(IndexType.VECTOR, indexType));
        }
    }
}
