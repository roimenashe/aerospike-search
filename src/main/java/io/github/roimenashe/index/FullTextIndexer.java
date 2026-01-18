package io.github.roimenashe.index;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import io.github.roimenashe.storage.AerospikeConnection;
import io.github.roimenashe.util.FullTextUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FullTextIndexer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FullTextIndexer.class);

    private final AerospikeConnection aerospikeConnection;
    private final Analyzer analyzer;
    private final Map<String, Directory> directories = new ConcurrentHashMap<>();
    private final Map<String, IndexWriter> writers = new ConcurrentHashMap<>();
    private final Map<String, IndexSearcher> searchers = new ConcurrentHashMap<>();

    public FullTextIndexer(AerospikeConnection aerospikeConnection) {
        this.aerospikeConnection = aerospikeConnection;
        this.analyzer = new StandardAnalyzer();
    }

    public void createFullTextIndex(String namespace, String set, String... binNames) throws Exception {
        String key = FullTextUtil.getFullTextUniqueIndexName(namespace, set);

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

        ScanCallback callback = (Key akey, Record record) -> {
            Document doc = new Document();
            String encodedId = Base64.getEncoder().encodeToString(akey.digest);
            doc.add(new StringField("id", encodedId, Field.Store.YES));

            record.bins.forEach((binName, value) -> {
                if (value instanceof String text && !text.isEmpty()) {
                    doc.add(new TextField(binName, text, Field.Store.YES));
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
        };

        if (binNames != null && binNames.length > 0) {
            aerospikeConnection.scan(namespace, set, callback, binNames);
        } else {
            aerospikeConnection.scan(namespace, set, callback);
        }

        writer.commit();
        closeOldSearcher(key);
        DirectoryReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);
        searchers.put(key, searcher);

        log.info("Indexed {} records for [{}:{}]", count.get(), namespace, set);
    }

    public Set<String> listFullTextIndexes() {
        return directories.keySet();
    }

    private void closeOldSearcher(String key) throws IOException {
        IndexSearcher oldSearcher = searchers.get(key);
        if (oldSearcher != null) {
            oldSearcher.getIndexReader().close();
        }
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
