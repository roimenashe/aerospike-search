package com.aerospike.fulltext;

import com.aerospike.AerospikeSearch;
import com.aerospike.BaseTest;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.model.IndexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class FullTextSearchTest extends BaseTest {

    @Test
    void testFullTextIndexAndSearch() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            search.createFullTextIndex(NAMESPACE, SET);

            // Search for the record that contains the word "Lucene"
            List<Record> results = search.searchText(NAMESPACE, SET, "Lucene", 10);

            results.forEach(System.out::println);
            Assertions.assertEquals(2, results.size());
        }
    }

    @Test
    void testHighLimit() throws Exception {
        AerospikeSearch search = new AerospikeSearch(aerospikeClient);

        search.createFullTextIndex(NAMESPACE, SET);

        // High limit should throw an exception
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> search.searchText(NAMESPACE, SET, "Lucene", 1000));
    }

    @Test
    void testFullTextFuzzySearch() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            search.createFullTextIndex(NAMESPACE, SET);

            // Perform fuzzy search — simulate a misspelling of "Lucene"
            List<Record> results = search.searchText(NAMESPACE, SET, "Lusene~1", 10);

            // Verify fuzzy match returns at least one doc
            results.forEach(System.out::println);
            Assertions.assertEquals(2, results.size());
        }
    }

    @Test
    void testMultipleFullTextIndexesAndSearches_DisjointTerms() throws Exception {
        final String usersSet = "users";
        final String productsSet = "products";

        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            // Users set - only mentions "Lucene"
            aerospikeClient.put(null, new Key(NAMESPACE, usersSet, "user1"),
                    new Bin("bio", "Lucene expert and search engineer"));
            aerospikeClient.put(null, new Key(NAMESPACE, usersSet, "user2"),
                    new Bin("bio", "Lucene tutorials and indexing tricks"));
            aerospikeClient.put(null, new Key(NAMESPACE, usersSet, "user3"),
                    new Bin("bio", "Check out this Lucene document"));

            // Products set - only mentions "Aerospike"
            aerospikeClient.put(null, new Key(NAMESPACE, productsSet, "prod1"),
                    new Bin("description", "High-performance Aerospike SSD server"));
            aerospikeClient.put(null, new Key(NAMESPACE, productsSet, "prod2"),
                    new Bin("description", "Enterprise-grade Aerospike appliance"));
            aerospikeClient.put(null, new Key(NAMESPACE, productsSet, "prod3"),
                    new Bin("description", "Aerospike is a real-time database"));
            aerospikeClient.put(null, new Key(NAMESPACE, productsSet, "prod4"),
                    new Bin("description", "NoSQL Databases: MongoDB, Aerospike and Redis"));

            search.createFullTextIndex(NAMESPACE, usersSet);
            search.createFullTextIndex(NAMESPACE, productsSet);

            // Queries that should hit only within their own set
            List<Record> usersLucene = search.searchText(NAMESPACE, usersSet, "Lucene", 10);
            Assertions.assertEquals(3, usersLucene.size());

            List<Record> productsAerospike = search.searchText(NAMESPACE, productsSet, "Aerospike", 10);
            Assertions.assertEquals(4, productsAerospike.size());

            // Cross-checks: should return no results because terms are disjoint
            List<Record> usersAerospike = search.searchText(NAMESPACE, usersSet, "Aerospike", 10);
            Assertions.assertTrue(usersAerospike.isEmpty(), "Expected no Aerospike match in users index");

            List<Record> productsLucene = search.searchText(NAMESPACE, productsSet, "Lucene", 10);
            Assertions.assertTrue(productsLucene.isEmpty(), "Expected no Lucene match in products index");
        }
    }

    @Test
    void testListFullTextIndexes() throws Exception {
        try (AerospikeSearch search = new AerospikeSearch(aerospikeClient)) {
            search.createFullTextIndex(NAMESPACE, SET);
            search.createFullTextIndex(NAMESPACE, "set2");
            search.createFullTextIndex(NAMESPACE, "set3");

            Map<String, IndexType> indexes = search.listIndexes();

            // Exactly 3 full-text indexes
            Assertions.assertEquals(3, indexes.size());
            indexes.forEach((s, indexType) -> Assertions.assertEquals(IndexType.FULL_TEXT, indexType));
        }
    }
}
