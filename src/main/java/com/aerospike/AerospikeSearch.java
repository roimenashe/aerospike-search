package com.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.index.FullTextIndexer;
import com.aerospike.index.VectorIndexer;
import com.aerospike.model.IndexType;
import com.aerospike.search.FullTextSearchService;
import com.aerospike.search.HybridSearchService;
import com.aerospike.search.VectorSearchService;
import com.aerospike.storage.AerospikeConnection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AerospikeSearch implements AutoCloseable {

    private final AerospikeConnection aerospikeConnection;
    private final FullTextIndexer fullTextIndexer;
    private final FullTextSearchService fullTextSearchService;
    private final VectorIndexer vectorIndexer;
    private final VectorSearchService vectorSearchService;
    private final HybridSearchService hybridSearchService;

    public AerospikeSearch(IAerospikeClient client) {
        this.aerospikeConnection = new AerospikeConnection(client);
        this.fullTextIndexer = new FullTextIndexer(aerospikeConnection);
        this.fullTextSearchService = new FullTextSearchService(fullTextIndexer);
        this.vectorIndexer = new VectorIndexer(aerospikeConnection);
        this.vectorSearchService = new VectorSearchService(vectorIndexer);
        this.hybridSearchService = new HybridSearchService(fullTextSearchService, vectorSearchService);
    }

    /**
     * Build or rebuild a full-text index from a given Aerospike namespace and set.
     *
     * @param namespace Aerospike namespace
     * @param set       Aerospike set
     */
    public void createFullTextIndex(String namespace, String set) throws Exception {
        fullTextIndexer.createFullTextIndex(namespace, set);
    }

    /**
     * Create or rebuild a vector index from a given Aerospike namespace, set and a vector bin.
     *
     * @param namespace Aerospike namespace
     * @param set       Aerospike set
     * @param vectorBin Vector Bin name
     */
    public void createVectorIndex(String namespace, String set, String vectorBin) throws Exception {
        vectorIndexer.createVectorIndex(namespace, set, vectorBin);
    }

    /**
     * Create or rebuild a vector index from a given Aerospike namespace, set and an embedding function.
     *
     * @param namespace Aerospike namespace
     * @param set       Aerospike set
     * @param embedder  Vector embedding function
     */
    public void createVectorIndex(String namespace, String set, Function<Record, float[]> embedder) throws Exception {
        vectorIndexer.createVectorIndex(namespace, set, embedder);
    }

    /**
     * List indexes.
     *
     * @return Map of existing indexes
     */
    public Map<String, IndexType> listIndexes() {
        Map<String, IndexType> indexes = new HashMap<>();
        fullTextIndexer.listFullTextIndexes().forEach(index -> indexes.put(index, IndexType.FULL_TEXT));
        vectorIndexer.listVectorIndexes().forEach(index -> indexes.put(index, IndexType.VECTOR));
        return indexes;
    }

    /**
     * Perform a full-text search over an in-memory index of a given Aerospike namespace and set.
     *
     * @param namespace Aerospike namespace
     * @param set       Aerospike set
     * @param query     Full-text query string
     * @param limit     Result limit
     * @return List of results
     */
    public List<Record> searchText(String namespace, String set, String query, int limit) throws Exception {
        if (limit > 100) {
            throw new IllegalArgumentException("limit must be smaller than 100");
        }
        List<String> encodedIds = fullTextSearchService.searchText(namespace, set, query, limit);
        return aerospikeConnection.fetchRecordsByDigest(namespace, set, encodedIds);
    }

    /**
     * Perform a vector search over an in-memory index of a given Aerospike namespace and set.
     *
     * @param namespace   Aerospike namespace
     * @param set         Aerospike set
     * @param queryVector Float query vector
     * @param k           The number of nearest neighbors to be retrieved for a given query
     * @return List of results
     */
    public List<Record> searchVector(String namespace, String set, float[] queryVector, int k) throws Exception {
        if (k > 100) {
            throw new IllegalArgumentException("K must be smaller than 100");
        }
        List<String> encodedIds = vectorSearchService.searchVector(namespace, set, queryVector, k);
        return aerospikeConnection.fetchRecordsByDigest(namespace, set, encodedIds);
    }

    /**
     * @param namespace    Aerospike namespace
     * @param set          Aerospike set
     * @param textQuery    Full-text query string
     * @param queryVector  Float query vector
     * @param limit        Result limit
     * @param textWeight   Full-Text weight in query (Float between 0-1, combined with vectorWeight should be 1)
     * @param vectorWeight Vector weight in query (Float between 0-1, combined with textWeight should be 1)
     * @return List of results
     */
    public List<Record> searchHybrid(String namespace, String set,
                                     String textQuery,
                                     float[] queryVector,
                                     int limit,
                                     double textWeight,
                                     double vectorWeight) throws Exception {
        List<String> encodedIds =
                hybridSearchService.searchHybrid(namespace, set, textQuery, queryVector, limit, textWeight, vectorWeight);
        return aerospikeConnection.fetchRecordsByDigest(namespace, set, encodedIds);
    }

    @Override
    public void close() throws Exception {
        fullTextIndexer.close();
        vectorIndexer.close();
    }
}
