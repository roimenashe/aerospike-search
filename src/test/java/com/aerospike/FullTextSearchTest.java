package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FullTextSearchTest {

    private static IAerospikeClient aerospikeClient;
    private static final String NAMESPACE = "test";
    private static final String SET = "docs";

    @BeforeAll
    static void setupAerospike() {
        // Connect to local Aerospike (assuming running on localhost:3000)
        aerospikeClient = new AerospikeClient("localhost", 3000);

        // Clear previous records
        aerospikeClient.truncate(null, NAMESPACE, SET, null);

        // Insert a few sample records
        WritePolicy wp = new WritePolicy();
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
    void testFullTextIndexAndSearch() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Build in-memory full-text index
            search.createFullTextIndex(NAMESPACE, SET);

            // Search for something that should match doc2
            List<Record> results = search.searchText(NAMESPACE, SET, "Lucene", 10);

            // Print and verify
            results.forEach(System.out::println);
            Assertions.assertFalse(results.isEmpty(), "Expected at least one result");
        }
    }

    @Test
    void testHighLimit() throws Exception {
        AerospikeSearch search = new AerospikeSearch(aerospikeClient);

        // Build in-memory full-text index
        search.createFullTextIndex(NAMESPACE, SET);

        // High limit should throw an exception
        Assertions.assertThrows(IllegalArgumentException.class, () -> search.searchText(NAMESPACE, SET, "Lucene", 1000));
    }

    @Test
    void testFullTextFuzzySearch() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Build the index
            search.createFullTextIndex(NAMESPACE, SET);

            // Insert some data
            Key key1 = new Key(NAMESPACE, SET, "doc1");
            Bin bin1 = new Bin("text", "Lucene search is powerful");
            aerospikeClient.put(null, key1, bin1);

            Key key2 = new Key(NAMESPACE, SET, "doc2");
            Bin bin2 = new Bin("text", "Lusene rocks!");
            aerospikeClient.put(null, key2, bin2);

            // Rebuild or update index after inserts
            search.createFullTextIndex(NAMESPACE, SET);

            // Perform fuzzy search — simulate a misspelling of "Lucene"
            List<Record> results = search.searchText(NAMESPACE, SET, "Lusene~1", 10);

            results.forEach(System.out::println);

            // Verify fuzzy match returns at least one doc
            Assertions.assertFalse(results.isEmpty(), "Expected fuzzy match for 'Lusene~1'");
        }
    }

    @Test
    void testMultipleFullTextIndexesAndSearches_DisjointTerms() throws Exception {
        final String SET_USERS = "users";
        final String SET_PRODUCTS = "products";

        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // --- Prepare test data (disjoint terms) ---
            // Users set -> only mentions "Lucene"
            aerospikeClient.put(null, new Key(NAMESPACE, SET_USERS, "user1"),
                    new Bin("bio", "Lucene expert and search engineer"));
            aerospikeClient.put(null, new Key(NAMESPACE, SET_USERS, "user2"),
                    new Bin("bio", "Lucene tutorials and indexing tricks"));

            // Products set -> only mentions "Aerospike"
            aerospikeClient.put(null, new Key(NAMESPACE, SET_PRODUCTS, "prod1"),
                    new Bin("description", "High-performance Aerospike SSD server"));
            aerospikeClient.put(null, new Key(NAMESPACE, SET_PRODUCTS, "prod2"),
                    new Bin("description", "Enterprise-grade Aerospike appliance"));

            // --- Build both indexes ---
            search.createFullTextIndex(NAMESPACE, SET_USERS);
            search.createFullTextIndex(NAMESPACE, SET_PRODUCTS);

            // --- Queries that should hit only within their own set ---
            List<Record> usersLucene = search.searchText(NAMESPACE, SET_USERS, "Lucene", 10);
            Assertions.assertFalse(usersLucene.isEmpty(), "Expected Lucene match in users index");

            List<Record> productsAerospike = search.searchText(NAMESPACE, SET_PRODUCTS, "Aerospike", 10);
            Assertions.assertFalse(productsAerospike.isEmpty(), "Expected Aerospike match in products index");

            // --- Cross-checks: should return no results because terms are disjoint ---
            List<Record> usersAerospike = search.searchText(NAMESPACE, SET_USERS, "Aerospike", 10);
            Assertions.assertTrue(usersAerospike.isEmpty(), "Expected no Aerospike match in users index");

            List<Record> productsLucene = search.searchText(NAMESPACE, SET_PRODUCTS, "Lucene", 10);
            Assertions.assertTrue(productsLucene.isEmpty(), "Expected no Lucene match in products index");
        }
    }
}
