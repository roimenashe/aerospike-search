package io.github.roimenashe.index;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import io.github.roimenashe.model.SimilarityFunction;
import io.github.roimenashe.storage.AerospikeConnection;
import io.github.roimenashe.util.VectorUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.github.roimenashe.util.VectorUtil.getVectorSimilarityFunction;

public class VectorIndexer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(VectorIndexer.class);

    private final AerospikeConnection aerospikeConnection;
    private final Map<String, Directory> directories = new ConcurrentHashMap<>();
    private final Map<String, IndexWriter> writers = new ConcurrentHashMap<>();
    private final Map<String, IndexSearcher> searchers = new ConcurrentHashMap<>();

    public VectorIndexer(AerospikeConnection aerospikeConnection) {
        this.aerospikeConnection = aerospikeConnection;
    }

    public void createVectorIndex(String namespace, String set, String vectorBinName,
                                  SimilarityFunction similarityFunction) throws Exception {
        Function<Record, float[]> extractor = record -> {
            Object raw = record.getValue(vectorBinName);
            switch (raw) {
                case null -> {
                    return null;
                }
                case byte[] bytes -> {
                    return VectorUtil.bytesToFloats(bytes);
                }
                case List<?> list -> {
                    float[] vector = new float[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        vector[i] = ((Number) list.get(i)).floatValue();
                    }
                    return vector;
                }
                default -> {
                }
            }
            return null;
        };

        long count = buildVectorIndex(namespace, set, similarityFunction, extractor, vectorBinName);
        log.info("Vector-indexed {} records (from bin '{}') for [{}:{}]", count, vectorBinName, namespace, set);
    }

    /**
     * Scans Aerospike records, computes vector embeddings via the supplied embedder,
     * and builds an in-memory Lucene vector index.
     */
    public void createVectorIndex(String namespace, String set,
                                  Function<Record, float[]> embedder, SimilarityFunction similarityFunction) throws Exception {
        long count = buildVectorIndex(namespace, set, similarityFunction, embedder);
        log.info("Vector-indexed {} records for [{}:{}]", count, namespace, set);
    }

    private long buildVectorIndex(String namespace, String set, SimilarityFunction similarityFunction,
                                  Function<Record, float[]> vectorExtractor, String... binNames) throws Exception {
        String key = VectorUtil.getUniqueVectorIndexName(namespace, set, similarityFunction);

        Directory directory = directories.computeIfAbsent(key, k -> new ByteBuffersDirectory());
        IndexWriter writer = writers.computeIfAbsent(key, k -> {
            try {
                return new IndexWriter(directory, new IndexWriterConfig());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writer.deleteAll();
        AtomicLong count = new AtomicLong();

        aerospikeConnection.scan(namespace, set, (Key akey, Record record) -> {
            float[] vector = vectorExtractor.apply(record);
            if (vector == null) return;

            Document doc = new Document();
            String encodedId = Base64.getEncoder().encodeToString(akey.digest);
            doc.add(new StringField("id", encodedId, Field.Store.YES));
            doc.add(new KnnFloatVectorField("vector", vector, getVectorSimilarityFunction(similarityFunction)));

            synchronized (writer) {
                try {
                    writer.addDocument(doc);
                    count.incrementAndGet();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, binNames);

        writer.commit();
        closeOldSearcher(key);
        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);
        searchers.put(key, searcher);

        return count.get();
    }

    private void closeOldSearcher(String key) throws IOException {
        IndexSearcher oldSearcher = searchers.get(key);
        if (oldSearcher != null) {
            oldSearcher.getIndexReader().close();
        }
    }

    public Set<String> listVectorIndexes() {
        return directories.keySet();
    }

    public IndexSearcher getIndexSearcher(String namespace, String set, SimilarityFunction similarityFunction) {
        return searchers.get(VectorUtil.getUniqueVectorIndexName(namespace, set, similarityFunction));
    }

    @Override
    public void close() throws IOException {
        for (IndexWriter writer : writers.values()) {
            writer.close();
        }
        for (Directory directory : directories.values()) {
            directory.close();
        }
    }
}
