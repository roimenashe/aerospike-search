package io.github.roimenashe.index;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import io.github.roimenashe.storage.AerospikeConnection;
import io.github.roimenashe.util.FullTextUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FullTextIndexer implements AutoCloseable {

    private final AerospikeConnection aerospikeConnection;
    private final Analyzer analyzer;
    private final Map<String, Directory> directories = new ConcurrentHashMap<>();
    private final Map<String, IndexWriter> writers = new ConcurrentHashMap<>();
    private final Map<String, IndexSearcher> searchers = new ConcurrentHashMap<>();

    public FullTextIndexer(AerospikeConnection aerospikeConnection) {
        this.aerospikeConnection = aerospikeConnection;
        this.analyzer = new StandardAnalyzer();
    }

    public void createFullTextIndex(String namespace, String set) throws Exception {
        String key = FullTextUtil.getFullTextUniqueIndexName(namespace, set);

        Directory directory = directories.computeIfAbsent(key, k -> new ByteBuffersDirectory());
        IndexWriter writer = writers.computeIfAbsent(key, k -> {
            try {
                return new IndexWriter(directory, new IndexWriterConfig(analyzer));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writer.deleteAll(); // clear old documents
        AtomicLong count = new AtomicLong();

        aerospikeConnection.scan(namespace, set, (Key akey, Record record) -> {
            Document doc = new Document();
            String encodedId = Base64.getEncoder().encodeToString(akey.digest);
            doc.add(new StringField("id", encodedId, Field.Store.YES));

            record.bins.forEach((binName, value) -> {
                if (value instanceof String text) {
                    if (!text.isEmpty()) {
                        doc.add(new TextField(binName, text, Field.Store.YES));
                    }
                }
            });

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

        System.out.printf("Indexed %d records for [%s:%s]%n", count.get(), namespace, set);
    }

    public Set<String> listFullTextIndexes() {
        return directories.keySet();
    }

    public IndexSearcher getIndexSearcher(String namespace, String set) {
        return searchers.get(FullTextUtil.getFullTextUniqueIndexName(namespace, set));
    }

    public Analyzer getAnalyzer() {
        return analyzer;
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
