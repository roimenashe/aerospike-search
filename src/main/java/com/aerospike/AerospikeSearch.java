package com.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.index.FullTextIndexer;
import com.aerospike.search.FullTextSearchService;
import com.aerospike.storage.AerospikeConnection;

import java.util.List;

public class AerospikeSearch implements AutoCloseable {

    private final AerospikeConnection aerospikeConnection;
    private final FullTextIndexer indexer;
    private final FullTextSearchService searchService;

    public AerospikeSearch(IAerospikeClient client) {
        this.aerospikeConnection = new AerospikeConnection(client);
        this.indexer = new FullTextIndexer(aerospikeConnection);
        this.searchService = new FullTextSearchService(indexer);
    }

    /**
     * Build or rebuild the full-text index from Aerospike.
     */
    public void createFullTextIndex(String namespace, String set) throws Exception {
        indexer.createFullTextIndex(namespace, set);
    }

    /**
     * Perform a full-text search over the in-memory index.
     */
    public List<Record> searchText(String namespace, String set, String query, int limit) throws Exception {
        if (limit > 100) {
            throw new IllegalArgumentException("limit must be smaller than 100");
        }
        List<String> encodedIds = searchService.searchText(namespace, set, query, limit);
        return aerospikeConnection.fetchRecordsByDigest(namespace, set, encodedIds);
    }

    @Override
    public void close() throws Exception {
        indexer.close();
    }
}
