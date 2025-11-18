package io.github.roimenashe;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import io.github.roimenashe.index.FullTextIndexer;
import io.github.roimenashe.index.VectorIndexer;
import io.github.roimenashe.model.IndexType;
import io.github.roimenashe.model.SimilarityFunction;
import io.github.roimenashe.search.FullTextSearchService;
import io.github.roimenashe.search.HybridSearchService;
import io.github.roimenashe.search.VectorSearchService;
import io.github.roimenashe.storage.AerospikeConnection;

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
     * Build or rebuild a full-text index.
     *
     * @param namespace Aerospike namespace
     * @param set       Aerospike set
     */
    public void createFullTextIndex(String namespace, String set) throws Exception {
        fullTextIndexer.createFullTextIndex(namespace, set);
    }

    /**
     * Create or rebuild a vector index on a vector Bin.
     *
     * @param namespace          Aerospike namespace
     * @param set                Aerospike set
     * @param vectorBin          Vector Bin name
     * @param similarityFunction Vector similarity function (e.g. EUCLIDEAN)
     */
    public void createVectorIndex(String namespace, String set, String vectorBin, SimilarityFunction similarityFunction) throws Exception {
        vectorIndexer.createVectorIndex(namespace, set, vectorBin, similarityFunction);
    }

    /**
     * Create or rebuild a vector index using an embedding function.
     *
     * @param namespace          Aerospike namespace
     * @param set                Aerospike set
     * @param embedder           Vector embedding function
     * @param similarityFunction Vector similarity function (e.g. EUCLIDEAN)
     */
    public void createVectorIndex(String namespace, String set, Function<Record, float[]> embedder, SimilarityFunction similarityFunction) throws Exception {
        vectorIndexer.createVectorIndex(namespace, set, embedder, similarityFunction);
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
     * Perform a full-text search.
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
     * Perform a vector search.
     *
     * @param namespace          Aerospike namespace
     * @param set                Aerospike set
     * @param queryVector        Float query vector
     * @param k                  The number of nearest neighbors to be retrieved for a given query
     * @param similarityFunction Vector similarity function (e.g. EUCLIDEAN)
     * @return List of results
     */
    public List<Record> searchVector(String namespace, String set, float[] queryVector, int k, SimilarityFunction similarityFunction) throws Exception {
        if (k > 100) {
            throw new IllegalArgumentException("K must be smaller than 100");
        }
        List<String> encodedIds = vectorSearchService.searchVector(namespace, set, queryVector, k, similarityFunction);
        return aerospikeConnection.fetchRecordsByDigest(namespace, set, encodedIds);
    }

    /**
     * Perform a hybrid search that combines full-text matching and vector similarity.
     * Full-text and vector scores are weighted and merged to produce a unified ranking.
     * Returns the top results ordered by hybrid relevance.
     *
     * @param namespace          Aerospike namespace
     * @param set                Aerospike set
     * @param textQuery          Full-text query string
     * @param queryVector        Float query vector
     * @param similarityFunction Vector similarity function (e.g. EUCLIDEAN)
     * @param limit              Result limit
     * @param textWeight         Full-Text weight in query (Float between 0-1, combined with vectorWeight should be 1)
     * @param vectorWeight       Vector weight in query (Float between 0-1, combined with textWeight should be 1)
     * @return List of results
     */
    public List<Record> searchHybrid(String namespace, String set,
                                     String textQuery,
                                     float[] queryVector,
                                     SimilarityFunction similarityFunction,
                                     int limit,
                                     double textWeight,
                                     double vectorWeight) throws Exception {
        List<String> encodedIds =
                hybridSearchService.searchHybrid(namespace, set, textQuery, queryVector, similarityFunction, limit, textWeight, vectorWeight);
        return aerospikeConnection.fetchRecordsByDigest(namespace, set, encodedIds);
    }

    @Override
    public void close() throws Exception {
        fullTextIndexer.close();
        vectorIndexer.close();
    }
}
