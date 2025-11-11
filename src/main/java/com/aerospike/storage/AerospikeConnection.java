package com.aerospike.storage;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class AerospikeConnection {

    private final IAerospikeClient client;

    public AerospikeConnection(IAerospikeClient client) {
        this.client = client;
    }

    /**
     * Fetch full records from Aerospike using their digests (as base64 strings).
     */
    public List<Record> fetchRecordsByDigest(String namespace, String set, List<String> encodedDigests) {
        List<Record> records = new ArrayList<>();
        Policy policy = new Policy();

        for (String encoded : encodedDigests) {
            byte[] digest = Base64.getDecoder().decode(encoded);
            Key key = new Key(namespace, digest, set, null);
            Record record = client.get(policy, key);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    public void scan(String namespace, String set, ScanCallback scanCallback) {
        ScanPolicy policy = new ScanPolicy();
        client.scanAll(policy, namespace, set, scanCallback);
    }
}
