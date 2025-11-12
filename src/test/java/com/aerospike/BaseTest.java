package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.Arrays;

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
        String titleBin = "title";
        String bodyBin = "body";
        String vectorBin = "vectorBin";

        // Insert a few sample records with short text fields
        aerospikeClient.put(wp, new Key(NAMESPACE, SET, "doc"),
                new Bin(titleBin, "Lucene in Action"),
                new Bin(bodyBin, "Full text search library for Java."),
                new Bin(vectorBin, Arrays.asList(1f, 0f, 1f)));

        aerospikeClient.put(wp, new Key(NAMESPACE, SET, "doc2"),
                new Bin(titleBin, "Aerospike and Lucene"),
                new Bin(bodyBin, "Combining Aerospike speed with Lucene indexing."),
                new Bin(vectorBin, Arrays.asList(0.8f, 0.2f, 1f)));

        aerospikeClient.put(wp, new Key(NAMESPACE, SET, "doc3"),
                new Bin(titleBin, "Distributed Databases"),
                new Bin(bodyBin, "Aerospike provides low latency storage."),
                new Bin(vectorBin, Arrays.asList(0f, 1f, 0f)));
    }

    @AfterAll
    static void tearDown() {
        aerospikeClient.truncate(null, NAMESPACE, SET, null);
        aerospikeClient.close();
    }
}
