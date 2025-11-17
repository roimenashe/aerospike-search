package com.aerospike.storage;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.Key;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ScanPolicy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class AerospikeConnection {

    private final IAerospikeClient client;

    public AerospikeConnection(IAerospikeClient client) {
        this.client = client;
    }

    public List<Record> fetchRecordsByDigest(String namespace, String set, List<String> encodedDigests) {
        List<Record> results = new ArrayList<>(encodedDigests.size());
        Key[] keys = new Key[encodedDigests.size()];

        for (int i = 0; i < encodedDigests.size(); i++) {
            byte[] digest = Base64.getDecoder().decode(encodedDigests.get(i));
            keys[i] = new Key(namespace, digest, set, null);
        }

        BatchPolicy batchPolicy = new BatchPolicy();
        Record[] records = client.get(batchPolicy, keys);

        for (Record record : records) {
            if (record != null) {
                results.add(record);
            }
        }
        return results;
    }

    public void scan(String namespace, String set, ScanCallback scanCallback, String... binNames) throws AerospikeException {
        ScanPolicy policy = new ScanPolicy();
        client.scanAll(policy, namespace, set, scanCallback, binNames);
    }
}
