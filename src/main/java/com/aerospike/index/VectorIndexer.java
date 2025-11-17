package com.aerospike.index;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.model.SimilarityFunction;
import com.aerospike.storage.AerospikeConnection;
import com.aerospike.util.VectorUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.aerospike.util.VectorUtil.getVectorSimilarityFunction;

public class VectorIndexer implements AutoCloseable {

    private final AerospikeConnection aerospikeConnection;
    private final Analyzer analyzer;
    private final Map<String, Directory> directories = new ConcurrentHashMap<>();
    private final Map<String, IndexWriter> writers = new ConcurrentHashMap<>();
    private final Map<String, IndexSearcher> searchers = new ConcurrentHashMap<>();

    public VectorIndexer(AerospikeConnection aerospikeConnection) {
        this.aerospikeConnection = aerospikeConnection;
        this.analyzer = new StandardAnalyzer();
    }

    public void createVectorIndex(String namespace, String set, String vectorBinName,
                                  SimilarityFunction similarityFunction) throws Exception {
        String key = VectorUtil.getUniqueVectorIndexName(namespace, set, similarityFunction);

        Directory directory = directories.computeIfAbsent(key, k -> new ByteBuffersDirectory());
        IndexWriter writer = writers.computeIfAbsent(key, k -> {
            try {
                return new IndexWriter(directory, new IndexWriterConfig(analyzer));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writer.deleteAll();
        AtomicLong count = new AtomicLong();

        aerospikeConnection.scan(namespace, set, (Key akey, Record record) -> {
            Document doc = new Document();
            String encodedId = Base64.getEncoder().encodeToString(akey.digest);
            doc.add(new StringField("id", encodedId, Field.Store.YES));

            Object raw = record.getValue(vectorBinName);
            if (raw == null) return;

            float[] vector;

            // Case 1: byte[]
            if (raw instanceof byte[] bytes) {
                vector = VectorUtil.bytesToFloats(bytes);
            }
            // Case 2: List<Number>
            else if (raw instanceof List<?> list) {
                vector = new float[list.size()];
                for (int i = 0; i < list.size(); i++) {
                    vector[i] = ((Number) list.get(i)).floatValue();
                }
            } else {
                // unsupported type
                return;
            }

            doc.add(new KnnFloatVectorField("vector", vector, getVectorSimilarityFunction(similarityFunction)));

            synchronized (writer) {
                try {
                    writer.addDocument(doc);
                    count.incrementAndGet();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, vectorBinName);

        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);
        searchers.put(key, searcher);

        System.out.printf("Vector-indexed %d records (from bin '%s') for [%s:%s]%n",
                count.get(), vectorBinName, namespace, set);
    }


    /**
     * Scans Aerospike records, computes vector embeddings via the supplied embedder,
     * and builds an in-memory Lucene vector index.
     */
    public void createVectorIndex(String namespace, String set,
                                  Function<Record, float[]> embedder, SimilarityFunction similarityFunction) throws Exception {
        String key = VectorUtil.getUniqueVectorIndexName(namespace, set, similarityFunction);

        Directory directory = directories.computeIfAbsent(key, k -> new ByteBuffersDirectory());
        IndexWriter writer = writers.computeIfAbsent(key, k -> {
            try {
                return new IndexWriter(directory, new IndexWriterConfig(analyzer));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writer.deleteAll(); // clear any previous index
        AtomicLong count = new AtomicLong();

        aerospikeConnection.scan(namespace, set, (Key akey, Record record) -> {
            Document doc = new Document();

            // Encode Aerospike digest as unique ID
            String encodedId = Base64.getEncoder().encodeToString(akey.digest);
            doc.add(new StringField("id", encodedId, Field.Store.YES));

            // Generate vector embedding for this record
            float[] vector = embedder.apply(record);
            if (vector == null) {
                return; // skip records without embeddings
            }

            // Add vector field
            doc.add(new KnnFloatVectorField("vector", vector, getVectorSimilarityFunction(similarityFunction)));

            synchronized (writer) {
                try {
                    writer.addDocument(doc);
                    count.incrementAndGet();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);
        searchers.put(key, searcher);

        System.out.printf("Vector-indexed %d records for [%s:%s]%n", count.get(), namespace, set);
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
