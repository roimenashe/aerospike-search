package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class BaseTest {
    protected static IAerospikeClient aerospikeClient;
    protected static final String NAMESPACE = "test";
    protected static final String SET = "docs";

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
}
