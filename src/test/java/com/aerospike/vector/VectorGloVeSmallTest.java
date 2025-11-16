package com.aerospike.vector;

import com.aerospike.AerospikeSearch;
import com.aerospike.BaseTest;
import com.aerospike.client.Record;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.util.VectorUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class VectorGloVeSmallTest extends BaseTest {

    private static final String gloveSet = "glove";
    private static Map<String, float[]> vectors;

    @BeforeAll
    static void loadDatasetIntoAerospike() throws Exception {
        // Clear previous data in the dedicated GloVe set
        aerospikeClient.truncate(null, NAMESPACE, gloveSet, null);

        vectors = loadCsvVectors("glove-mini-500-50d.csv", 50);

        WritePolicy wp = new WritePolicy();

        for (Map.Entry<String, float[]> entry : vectors.entrySet()) {
            String token = entry.getKey();

            aerospikeClient.put(
                    wp,
                    new Key(NAMESPACE, gloveSet, token),
                    // Store both the token (for testing) and the vector
                    new Bin("token", token),
                    new Bin("vectorBin", VectorUtil.floatsToBytes(entry.getValue()))
            );
        }
    }

    @Test
    void testVectorKnnSearchTop5() throws Exception {
        // pick one realistic "search" vector as the query
        String vectorKey = vectors.keySet().stream()
                .filter(k -> k.startsWith("search"))
                .findFirst()
                .orElseThrow();

        float[] query = vectors.get(vectorKey);

        // ---- Compute ground truth ranking (DOT_PRODUCT) ----
        record Scored(String token, float score) {
        }

        List<Scored> expectedTop5 = vectors.entrySet().stream()
                .map(e -> new Scored(
                        e.getKey(),
                        dot(query, e.getValue())  // compute similarity
                ))
                .sorted((a, b) -> Float.compare(b.score, a.score)) // desc
                .limit(5)
                .toList();

        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {

            search.createVectorIndex(NAMESPACE, gloveSet, "vectorBin");

            List<Record> results = search.searchVector(NAMESPACE, gloveSet, query, 5);

            Assertions.assertEquals(5, results.size());

            // ---- Check exact ranking match ----
            for (int i = 0; i < 5; i++) {
                String actual = results.get(i).getString("token");
                String expected = expectedTop5.get(i).token();

                Assertions.assertEquals(
                        expected,
                        actual,
                        "Rank " + (i + 1) + " mismatch: expected " + expected + " but got " + actual
                );
            }
        }
    }

    /**
     * Compute dot-product for KNN ranking
     */
    private static float dot(float[] a, float[] b) {
        float s = 0f;
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }

    private static Map<String, float[]> loadCsvVectors(String resource, int dim) throws IOException {
        Map<String, float[]> map = new HashMap<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        Objects.requireNonNull(
                                VectorGloVeSmallTest.class.getResourceAsStream("/" + resource),
                                "Resource not found: " + resource
                        )
                ))) {

            String line;
            // assuming first line is a header, skip if needed
            // comment this if your CSV has no header
            line = br.readLine();
            if (line == null) {
                return map;
            }

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String key = parts[0];

                float[] vec = new float[dim];
                for (int i = 1; i <= dim; i++) {
                    vec[i - 1] = Float.parseFloat(parts[i]);
                }
                map.put(key, vec);
            }
        }
        return map;
    }
}
